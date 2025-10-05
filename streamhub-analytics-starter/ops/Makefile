
SHELL := /bin/bash

.PHONY: install run-ingest run-stream run-dashboard gen-load

install:
	python3 -m venv .venv && source .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

run-ingest:
	go run ingest-go/cmd/server/main.go

run-stream:
	python streaming-py/streaming.py

run-dashboard:
	streamlit run dashboard/streamlit_app.py

gen-load:
	python loadgen_py/generate_events.py --rate 200 --batch 100
