# rps-analytics-sandbox

# RPS Analytics Sandbox (Docker + Postgres + dbt + Streamlit + Metabase)

A synthetic, Switzerland-flavored pharma analytics environment to practice SQL, GTN waterfalls,
brand performance, forecast vs actuals, and dashboarding — tailored for a Business Data Analyst case.

## Stack
- **Postgres 16** – warehouse
- **dbt (Postgres)** – transforms/tests/marts
- **Python data generator** – realistic synthetic data
- **Streamlit** – dashboards (Exec & Brand)
- **Metabase** – ad-hoc BI exploration

## Quick start
Prereqs: Docker Desktop.

```bash
cp .env.example .env
docker compose build
docker compose up -d postgres
docker compose run --rm -e SCALE=small generator
docker compose run --rm dbt bash -lc "dbt deps && dbt build"
docker compose up -d streamlit metabase

````

- Streamlit: http://localhost:8501
- Metabase: http://localhost:3000


## License

This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.