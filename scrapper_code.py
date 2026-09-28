import json
import csv
import argparse
import re
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.fotmob.com"
LEAGUE_ID = 268
DEFAULT_SEASON = 2024
EXPECTED_MATCHES = 380
OUTPUT_DIR = Path(__file__).resolve().parent
MASTER_PATH = OUTPUT_DIR / "fotmob_rating_all_matches.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
}

CSV_COLUMNS = [
    "match_url",
    "match_id",
    "match_time_utc",
    "home_team",
    "away_team",
    "player_id",
    "player_name",
    "team_name",
    "is_goalkeeper",
    "position_id",
    "rating",
    "season",
    "match_year",
]
AUDIT_COLUMNS = [
    "season",
    "match_id",
    "match_time_utc",
    "home_team",
    "away_team",
    "coverage_level",
    "ratings_count",
    "status",
]


def create_session():
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(HEADERS)
    session.mount("https://", adapter)
    return session


SESSION = create_session()


def get_json(url, params=None):
    response = SESSION.get(url, params=params, timeout=40)
    response.raise_for_status()
    return response.json()


def get_season_fixtures(season):
    """Lê de uma só vez os fixtures oficiais da temporada e conserva cada match ID."""
    url = f"{BASE_URL}/pt-BR/leagues/{LEAGUE_ID}/fixtures/serie"
    response = SESSION.get(
        url,
        params={"season": season, "group": "by-round", "round": 0},
        timeout=40,
    )
    response.raise_for_status()
    next_data = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        response.text,
        re.DOTALL,
    )
    if not next_data:
        raise RuntimeError("A página de fixtures não contém o JSON __NEXT_DATA__ esperado.")
    payload = json.loads(next_data.group(1))
    fixtures = payload["props"]["pageProps"]["fixtures"]["allMatches"]

    season_fixtures = [
        fixture
        for fixture in fixtures
        if str(fixture.get("status", {}).get("utcTime", "")).startswith(str(season))
    ]
    ids = [str(fixture.get("id", "")) for fixture in season_fixtures]

    if len(season_fixtures) != EXPECTED_MATCHES:
        raise RuntimeError(
            f"Esperava {EXPECTED_MATCHES} partidas de {season}, "
            f"mas a FotMob retornou {len(season_fixtures)}. CSV não será substituído."
        )
    if not all(ids) or len(ids) != len(set(ids)):
        raise RuntimeError("A lista de fixtures contém match IDs ausentes ou repetidos.")

    return season_fixtures


def fetch_match_details(match_id):
    """Consulta os detalhes pelo ID numérico, não pelo slug potencialmente repetido."""
    return get_json(
        f"{BASE_URL}/api/data/matchDetails",
        params={"matchId": match_id, "ccode3": "BRA", "language": "pt-BR"},
    )


def extract_ratings(fixture, details, season):
    general = details.get("general", {})
    expected_id = str(fixture["id"])
    actual_id = str(general.get("matchId", ""))
    if actual_id != expected_id:
        raise ValueError(
            f"A API devolveu match ID {actual_id}; era esperado {expected_id}."
        )

    match_date = general.get("matchTimeUTCDate") or fixture.get("status", {}).get("utcTime", "")
    match_year = re.match(r"(\d{4})", str(match_date))
    if not match_year:
        raise ValueError(f"Data de partida inválida: {match_date!r}.")

    home_team = general.get("homeTeam", {}).get("name") or fixture.get("home", {}).get("name")
    away_team = general.get("awayTeam", {}).get("name") or fixture.get("away", {}).get("name")
    match_url = urljoin(BASE_URL, fixture.get("pageUrl", ""))
    rows = []
    player_stats = details.get("content", {}).get("playerStats", {})

    # Algumas partidas têm coverage "lower" e a API publica os detalhes
    # gerais, mas nenhum rating individual. Não são falhas de parsing.
    if player_stats is None:
        return rows

    if not isinstance(player_stats, dict):
        raise ValueError("A resposta não contém playerStats no formato esperado.")

    for player in player_stats.values():
        if not isinstance(player, dict):
            continue

        for group in player.get("stats", []):
            if not isinstance(group, dict):
                continue
            metrics = group.get("stats", {})
            if not isinstance(metrics, dict):
                continue

            for metric_name, metric in metrics.items():
                if metric_name.strip().casefold() != "fotmob rating":
                    continue
                if not isinstance(metric, dict):
                    continue
                rating = metric.get("stat", {}).get("value")
                if rating is None:
                    continue

                rows.append(
                    {
                        "match_url": match_url,
                        "match_id": expected_id,
                        "match_time_utc": match_date,
                        "home_team": home_team,
                        "away_team": away_team,
                        "player_id": player.get("id"),
                        "player_name": player.get("name"),
                        "team_name": player.get("teamName"),
                        "is_goalkeeper": player.get("isGoalkeeper"),
                        "position_id": player.get("positionId"),
                        "rating": rating,
                        "season": season,
                        "match_year": int(match_year.group(1)),
                    }
                )

    return rows


def write_csv_atomically(rows, output_path):
    """Só troca o CSV anterior depois de a coleta passar pelas validações."""
    temporary_path = output_path.with_suffix(".tmp.csv")
    with temporary_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    temporary_path.replace(output_path)


def write_audit_atomically(rows, season):
    audit_path = OUTPUT_DIR / f"fotmob_match_coverage_{season}.csv"
    temporary_path = audit_path.with_suffix(".tmp.csv")
    with temporary_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=AUDIT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    temporary_path.replace(audit_path)
    return audit_path


def read_csv_rows(path):
    with path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            return []
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
        return [
            {
                key: value.strip() if isinstance(value, str) else value
                for key, value in row.items()
            }
            for row in reader
        ]


def seed_2025_annual_file():
    """Migra o consolidado anterior para o formato anual, validando antes de copiar."""
    annual_path = OUTPUT_DIR / "fotmob_rating_2025.csv"
    if annual_path.exists() or not MASTER_PATH.exists():
        return

    old_rows = read_csv_rows(MASTER_PATH)
    if not old_rows or any(row.get("season") != "2025" for row in old_rows):
        raise RuntimeError(
            "O consolidado existente não parece ser somente a temporada 2025; "
            "não vou usá-lo para criar o arquivo anual."
        )
    old_match_ids = {row.get("match_id") for row in old_rows}
    old_keys = [(row.get("match_id"), row.get("player_id")) for row in old_rows]
    if len(old_match_ids) != EXPECTED_MATCHES or len(old_keys) != len(set(old_keys)):
        raise RuntimeError(
            "O consolidado existente não passou pela validação para migração anual."
        )

    write_csv_atomically(old_rows, annual_path)
    print(f"Temporada 2025 preservada como {annual_path.name}.")


def rebuild_master():
    seed_2025_annual_file()
    annual_paths = sorted(OUTPUT_DIR.glob("fotmob_rating_20??.csv"))
    combined_rows = []
    for annual_path in annual_paths:
        combined_rows.extend(read_csv_rows(annual_path))

    unique_rows = {}
    for row in combined_rows:
        key = (str(row.get("season")), str(row.get("match_id")), str(row.get("player_id")))
        unique_rows[key] = row
    final_rows = list(unique_rows.values())
    final_rows.sort(
        key=lambda row: (
            str(row.get("season", "")),
            str(row.get("match_time_utc", "")),
            str(row.get("match_id", "")),
            -float(row.get("rating") or 0),
        )
    )
    write_csv_atomically(final_rows, MASTER_PATH)
    return len(final_rows), annual_paths


def main(season=DEFAULT_SEASON):
    if not 2000 <= season <= 2099:
        raise ValueError("Informe um ano de temporada válido, entre 2000 e 2099.")

    annual_path = OUTPUT_DIR / f"fotmob_rating_{season}.csv"
    fixtures = get_season_fixtures(season)
    print(f"Fixtures confirmados pela FotMob: {len(fixtures)} partidas da temporada {season}.")

    all_rows = []
    failed_matches = []
    audit_rows = []

    for index, fixture in enumerate(fixtures, start=1):
        match_id = str(fixture["id"])
        try:
            details = fetch_match_details(match_id)
            rows = extract_ratings(fixture, details, season)
            all_rows.extend(rows)
            general = details.get("general", {})
            audit_rows.append(
                {
                    "season": season,
                    "match_id": match_id,
                    "match_time_utc": general.get("matchTimeUTCDate")
                    or fixture.get("status", {}).get("utcTime", ""),
                    "home_team": general.get("homeTeam", {}).get("name")
                    or fixture.get("home", {}).get("name"),
                    "away_team": general.get("awayTeam", {}).get("name")
                    or fixture.get("away", {}).get("name"),
                    "coverage_level": general.get("coverageLevel"),
                    "ratings_count": len(rows),
                    "status": "ratings_available" if rows else "no_player_ratings_published",
                }
            )
        except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
            failed_matches.append((match_id, str(exc)))

        if index % 20 == 0 or index == len(fixtures):
            print(
                f"[{index}/{len(fixtures)}] ratings coletados: {len(all_rows)}; "
                f"falhas: {len(failed_matches)}"
            )
        time.sleep(0.15)

    if failed_matches:
        print("[INCOMPLETO] CSV anterior preservado; não publiquei uma coleta parcial.")
        print(f"Partidas com erro de consulta/estrutura: {len(failed_matches)}")
        for match_id, error in failed_matches[:10]:
            print(f"  match_id {match_id}: {error}")
        return

    # Uma linha por jogador em cada partida; IDs preservam jogos de ida e volta distintos.
    unique_rows = {}
    for row in all_rows:
        key = (row["match_id"], str(row["player_id"]))
        unique_rows[key] = row
    final_rows = list(unique_rows.values())
    final_rows.sort(
        key=lambda row: (row["match_time_utc"], row["match_id"], -float(row["rating"]))
    )

    match_ids_with_ratings = {row["match_id"] for row in final_rows}
    successfully_fetched_ids = {row["match_id"] for row in audit_rows}
    if len(successfully_fetched_ids) != EXPECTED_MATCHES:
        raise RuntimeError(
            f"Detalhes coletados de {len(successfully_fetched_ids)} de "
            f"{EXPECTED_MATCHES} partidas. CSV anterior preservado."
        )

    write_csv_atomically(final_rows, annual_path)
    audit_path = write_audit_atomically(audit_rows, season)
    total_rows, annual_paths = rebuild_master()
    no_rating_ids = [row["match_id"] for row in audit_rows if not row["ratings_count"]]
    print("[SUCESSO] Todas as partidas foram consultadas; CSV de ratings sem duplicatas.")
    print(
        f"Partidas consultadas: {len(successfully_fetched_ids)} | "
        f"Com ratings: {len(match_ids_with_ratings)} | Ratings: {len(final_rows)}"
    )
    if no_rating_ids:
        print(f"Sem ratings publicados pela FotMob: {len(no_rating_ids)} partida(s): {', '.join(no_rating_ids)}")
    print(f"Arquivo anual: {annual_path.name}")
    print(f"Auditoria por partida: {audit_path.name}")
    print(f"Consolidado: {MASTER_PATH.name} | {total_rows} ratings em {len(annual_paths)} temporada(s)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coleta FotMob ratings do Brasileirão por temporada.")
    parser.add_argument(
        "season",
        nargs="?",
        type=int,
        default=DEFAULT_SEASON,
        help=f"ano da temporada a coletar (padrão: {DEFAULT_SEASON})",
    )
    main(parser.parse_args().season)
