[README.md](https://github.com/user-attachments/files/22705031/README.md)

# streamhub-analytics

A hybrid SWE + Data Engineering project: ingest user watch events via a Go service, process them in a Python streaming job, write Parquet to a local lake, and visualize metrics with a Streamlit dashboard. Designed to be simple to run locally (no Kafka required) but structured so you can later swap in Kafka/Redpanda.

## Quickstart

```bash
# 1) Create & activate a virtualenv (Mac/Linux)
python3 -m venv .venv && source .venv/bin/activate
# Windows (PowerShell):  python -m venv .venv; .venv\Scripts\Activate.ps1

# 2) Install deps
pip install -r requirements.txt

# 3) Start Go ingest server (terminal A)
go run ingest-go/cmd/server/main.go

# 4) Start Python streaming job (terminal B)
python streaming-py/streaming.py

# 5) Start dashboard (terminal C)
streamlit run dashboard/streamlit_app.py

# 6) Generate load (terminal D) - optional
python loadgen_py/generate_events.py --rate 200
```

Then open the dashboard URL printed by Streamlit and watch metrics update.
