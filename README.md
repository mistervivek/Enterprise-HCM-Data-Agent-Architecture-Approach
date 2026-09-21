# 🚀 Enterprise HCM Data Agent

An autonomous, human-in-the-loop ETL migration agent designed to ingest messy HR/CRM exports, semantically map them to a strict target schema, and push valid records to a target API. 

## Tech Stack
* **Frontend & Orchestration:** Gradio
* **Data Processing & ETL:** Pandas
* **Schema Enforcement:** Python

## Architecture & Flow
1. **Ingestion:** Upload raw CSV or Excel files.
2. **AI Profiling:** The agent probabilistically maps columns to the target schema.
3. **Escalation Queue:** Ambiguous or low-confidence mappings are caught and surfaced to the UI.
4. **Human Review:** A consultant manually resolves the ambiguity via a structured JSON interface.
5. **Deterministic ETL:** Pandas executes rigorous normalization, deduplication, and validation.
6. **Audit & Upload:** Clean data is pushed to a mock API, generating a granular JSON audit trail.
