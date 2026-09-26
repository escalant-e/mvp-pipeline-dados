# MVP: Pipeline de Dados na Nuvem - Futebol Analytics

Este repositório contém a implementação completa de um pipeline de dados em nuvem estruturado sobre a **Arquitetura Medalhão (Bronze, Silver e Gold)** no **Databricks Free Edition**, utilizando o **Unity Catalog** para governança e o **Delta Lake** como camada de armazenamento transacional ACID.

---

## 1. Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

### Contexto do Problema

No futebol profissional contemporâneo, a tomada de decisão em contratações, precificação de atletas e análise de desempenho é fortemente orientada a dados. Clubes e analistas de mercado necessitam cruzar dados estatísticos de jogo com dados de transferências e valuation para identificar jogadores subvalorizados, prever oscilações de valor de mercado e avaliar a eficiência técnica de atletas sob diferentes recortes de minutagem e idade.

Este projeto visa integrar o histórico financeiro e transacional de atletas das principais ligas do mundo com métricas avaliativas de partidas individuais (notas estatísticas por jogo), construindo um repositório analítico centralizado na nuvem.

### Perguntas de Negócio a serem Respondidas

1. **Impacto da Nota em Partidas no Valor de Mercado:** Atletas com notas médias por temporada superiores a 7.0 apresentam correlação positiva direta com seu valuation de mercado?
2. **Distribuição de Desempenho por Idade:** Em qual faixa etária os jogadores atingem a nota média máxima nas principais competições europeias?
3. **Eficiência e Minutagem:** Jogadores reservas com alta eficiência de participações diretas em gols (gols + assistências por minuto) sustentam notas médias compatíveis com atletas titulares consolidados?
4. **Disparidade Financeira entre Ligas:** Qual é a diferença na relação custo-benefício (milhões de euros por ponto de nota média) entre as cinco principais ligas europeias e as ligas periféricas?
5. **Evolução Temporal de Talentos:** Quais atletas sub-21 apresentaram a maior curva de valorização financeira concomitante à evolução estatística de notas entre 2022 e 2025?

### Estrutura dos Dados Brutos

O projeto consolida dados originados de duas fontes distintas:

1. **Dataset Kaggle (Transfermarkt Football Dataset):**
   - `players`: Cadastro mestre do atleta (código, data de nascimento, posição, clube, altura, valor de mercado).
   - `appearances`: Registro detalhado por partida disputada (minutos jogados, gols, assistências, cartões).
   - `games`, `clubs`, `competitions`, `club_games`, `countries`, `game_events`, `game_lineups`, `national_teams`, `player_valuations`, `transfers`.

2. **Dataset FotMob (Ratings 2022 a 2025):**
   - `fotmob_rating_2022`
   - `fotmob_rating_2023`
   - `fotmob_rating_2024`
   - `fotmob_rating_2025`
   - Colunas-chave:
     - `match_id`
     - `match_time_utc`
     - `player_name`
     - `team_name`
     - `position_id`
     - `rating`
     - `is_goalkeeper`
     - `season`
     - `match_year`

### Licença dos Dados

- **Dataset Transfermarkt (Kaggle):** Licenciado sob Open Data Commons Open Database License (ODbL) / CC0 Public Domain.
- **Dataset FotMob:** Dados públicos coletados exclusivamente para fins acadêmicos, educacionais e de pesquisa científica.
- **Código do Projeto:** Licença MIT.

---

## 2. Carga dos Dados (Etapa 4.2)

### Infraestrutura de Nuvem e Desafios de Conectividade

O pipeline opera no **Databricks Free Edition**, sustentado pela infraestrutura da **AWS**.

Durante o desenvolvimento da etapa de ingestão, foi diagnosticado que o plano gratuito do Databricks aplica restrições de resolução de DNS para a rede externa aberta (`gaierror: [Errno -3] Temporary failure in name resolution`), impedindo requisições HTTP diretas e rotinas de web scraping tradicional nos notebooks.

```text
HTTPSConnectionPool(host='www.sofascore.com', port=443): Max retries exceeded with url: /api/v1/...
(Caused by NameResolutionError: Failed to resolve 'www.sofascore.com')
```

Para contornar essa limitação mantendo o rigor de engenharia de dados em nuvem, a ingestão foi segregada em dois fluxos complementares:

1. **Ingestão API Kaggle:** Download programático via biblioteca `kaggle`, persistindo as 12 tabelas Delta Lake diretamente no schema `bronze`.
2. **Ingestão via Volumes do Unity Catalog:** Upload dos arquivos FotMob para:

```text
/Volumes/futebol_analytics/bronze/raw_files/
```

A partir dessa landing zone, um notebook PySpark realizou a leitura em lote e persistiu cada arquivo como tabela Delta nativa, adicionando:

- `_ingested_at`
- `_source_file`

### Referência aos Scripts no GitHub

- `00_setup_governanca_catalogo.ipynb`
- `01_bronze_ingestao_kaggle.ipynb`
- `01_bronze_ingestao_fotmob.ipynb`

---

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)

### Arquitetura Medalhão e Unity Catalog

- **Catálogo:** `futebol_analytics`
- **Camada Bronze:** 16 tabelas Delta + volume `raw_files`
- **Camada Silver:** Dados tratados, deduplicados e enriquecidos
- **Camada Gold:** Modelo dimensional em **Star Schema**

```text
                     ┌──────────────────────┐
                     │   dim_tempo (Gold)   │
                     └──────────┬───────────┘
                                │
 ┌─────────────────────┐        │        ┌──────────────────────┐
 │ dim_jogadores (Gold)├────────┼────────┤ dim_clubes (Gold)    │
 └─────────────────────┘        │        └──────────────────────┘
                                │
                     ┌──────────┴───────────┐
                     │ fato_desempenho      │
                     │      (Gold)          │
                     └──────────────────────┘
```

### Catálogo de Dados das Entidades Principais

| Tabela | Camada | Coluna | Tipo | Descrição |
|---------|---------|---------|---------|---------|
| fotmob_ratings | Silver | fotmob_player_id | Integer | Identificador FotMob |
| fotmob_ratings | Silver | player_name_clean | String | Nome padronizado |
| fotmob_ratings | Silver | rating | Double | Nota da partida |
| fotmob_ratings | Silver | match_year | Integer | Ano da partida |
| players | Silver | player_id | Long | ID Transfermarkt |
| players | Silver | date_of_birth | Date | Data nascimento |
| players | Silver | market_value_in_eur | Double | Valor de mercado |
| players | Silver | position | String | Posição |
| appearances | Silver | appearance_id | String | ID participação |
| appearances | Silver | minutes_played | Long | Minutos jogados |
| fato_desempenho | Gold | sk_fato | Long | Chave substituta |
| fato_desempenho | Gold | nota_media_ano | Double | Média anual |
| fato_desempenho | Gold | total_gols | Long | Total de gols |

> *(Inserir Screenshot 1: Unity Catalog com bronze, silver, gold e volume raw_files)*

---

## 4. Pipeline de Dados (Etapa 4.4)

### Organização dos Processos de ETL

1. **00_setup_governanca_catalogo.ipynb**
   - Provisionamento
   - Migração via `DEEP CLONE`
   - Testes de paridade

2. **01_bronze_ingestao_kaggle.ipynb**
3. **01_bronze_ingestao_fotmob.ipynb**
   - Ingestão bruta
   - Metadados de auditoria

4. **02_silver_limpeza_qualidade.ipynb**
   - `unionByName`
   - Conversão de datas
   - Deduplicação
   - Padronização textual

5. **03_gold_modelagem_dimensional.ipynb**
   - Construção das dimensões
   - Construção da fato
   - Chaves substitutas

6. **04_analise_dados.ipynb**
   - Consultas analíticas SQL

### Persistência dos Dados

Todas as tabelas foram gravadas em **Delta Lake** utilizando `saveAsTable`, garantindo:

- ACID
- Time Travel
- Compressão Parquet

> *(Inserir Screenshot 2: tabelas Bronze)*
>
> *(Inserir Screenshot 3: tabelas Silver)*

---

## 5. Qualidade de Dados (Etapa 4.5)

### Completude

**Problema:**
- Valores nulos em `sub_position` e `foot`

**Tratamento:**
- `.fillna("Unknown")`
- Remoção de registros críticos incompletos

### Unicidade

**Problema:**
- Possíveis duplicações

**Tratamento:**

```python
.dropDuplicates(["match_id", "player_id"])
```

```python
.dropDuplicates(["player_id"])
```

### Consistência e Padronização

**Problema:**
- Datas em string
- Grafias inconsistentes

**Tratamento:**

```python
to_date()
```

```python
lower(trim(col("player_name")))
```

### Acurácia

**Problema:**
- Ratings inválidos

**Tratamento:**

```python
col("rating").between(0, 10)
```

```python
round()
```

### Outliers e Regras de Negócio

**Problema:**
- Minutagem negativa

**Tratamento:**

```python
minutes_played >= 0
```

---

## 6. Análise de Dados

### Pergunta 1: Impacto da Nota em Partidas no Valor de Mercado

#### Consulta SQL

```sql
SELECT
    CASE
        WHEN f.nota_media_ano >= 7.5 THEN 'Elite (>= 7.5)'
        WHEN f.nota_media_ano >= 7.0 THEN 'Alto (7.0 - 7.49)'
        WHEN f.nota_media_ano >= 6.5 THEN 'Médio (6.5 - 6.99)'
        ELSE 'Abaixo da Média (< 6.5)'
    END AS faixa_desempenho,
    COUNT(DISTINCT f.player_id) AS total_jogadores,
    ROUND(AVG(d.market_value_in_eur) / 1000000, 2) AS valor_medio_milhoes_eur,
    ROUND(AVG(f.total_gols), 2) AS media_gols_temporada
FROM gold.fato_desempenho_jogadores f
INNER JOIN gold.dim_jogadores d
    ON f.player_id = d.player_id
WHERE d.market_value_in_eur IS NOT NULL
GROUP BY 1
ORDER BY valor_medio_milhoes_eur DESC;
```

#### Discussão

Os dados indicam que atletas com notas médias superiores a 7.5 apresentam valuation significativamente superior às demais faixas, sugerindo correlação positiva entre desempenho estatístico e valor de mercado.

> *(Inserir Screenshot 4)*

---

### Pergunta 2: Distribuição de Desempenho por Faixa Etária

#### Consulta SQL

```sql
SELECT
    CASE
        WHEN (2025 - YEAR(d.date_of_birth)) < 21 THEN 'Sub-21'
        WHEN (2025 - YEAR(d.date_of_birth)) BETWEEN 21 AND 25 THEN '21-25 anos'
        WHEN (2025 - YEAR(d.date_of_birth)) BETWEEN 26 AND 30 THEN '26-30 anos'
        ELSE 'Acima de 30 anos'
    END AS faixa_etaria,
    ROUND(AVG(f.nota_media_ano), 2) AS nota_media,
    ROUND(AVG(f.minutos_jogados), 0) AS media_minutos,
    COUNT(*) AS total_registros
FROM gold.fato_desempenho_jogadores f
INNER JOIN gold.dim_jogadores d
    ON f.player_id = d.player_id
WHERE d.date_of_birth IS NOT NULL
GROUP BY 1
ORDER BY nota_media DESC;
```

#### Discussão

A faixa de 26–30 anos concentra as maiores médias de desempenho, refletindo o pico de maturidade atlética e tática. Já atletas Sub-21 tendem a apresentar maior potencial de valorização.

> *(Inserir Screenshot 5)*

---

## 7. Autoavaliação

### Atingimento dos Objetivos

O pipeline foi implementado integralmente na nuvem, desde a ingestão até a camada analítica Gold, atendendo aos requisitos definidos para o MVP.

### Dificuldades Encontradas

#### Bloqueio de DNS no Databricks

Solução:
- Landing Zone com Unity Catalog Volumes

#### Governança e Migração

Solução:
- Catálogo dedicado `futebol_analytics`
- Migração via `DEEP CLONE`

#### Git Folders

Solução:
- Integração com GitHub por meio de Git Folder dedicado

### Trabalhos Futuros

- Databricks Workflows (Jobs)
- Great Expectations
- Delta Live Tables (DLT)
- Dashboard Power BI
- Dashboard Databricks Dashboards