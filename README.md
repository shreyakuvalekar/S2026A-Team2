# DataWeave — AI-Powered ETL Pipeline

DataWeave is a multi-agent ETL (Extract, Transform, Load) system built with LangGraph. A team of four AI agents works in sequence to extract data from a source, plan and execute transformations, and load the result into a database or file. The pipeline is oriented around **locally-hosted LLMs via Ollama** no OpenAI API key or cloud AI service required.

---

## Architecture

```
source (CSV / API)
       ↓
    Scout          deterministic — reads data, infers schema
       ↓
  Architect        LLM — generates a plain-English transformation plan
       ↓
   Engineer        LLM — writes and executes Python transformation code
       ↓
  [router]         pass → Loader | retry → Engineer | escalate → Architect
       ↓
    Loader         deterministic — writes to CSV, JSON, SQLite, or PostgreSQL
```

All agents share a single `ETLState` object. Every step appends to an `audit_log` for full traceability.

---

## Local Model Orientation

DataWeave is designed to run **entirely on local hardware**  the LLM agents (Architect and Engineer) call an Ollama instance rather than a cloud API. This means:

- Your data never leaves your machine or network
- No per-token costs
- You can swap models by changing one environment variable

The default model is **`qwen2.5:14b-instruct-q4_K_M`**, a capable 14B-parameter instruction model that fits in ~10 GB of VRAM and handles code generation well.

### Installing Ollama

1. Download and install Ollama from [ollama.com](https://ollama.com)
2. Pull the model used by the agents:

```bash
ollama pull qwen2.5:14b-instruct-q4_K_M
```

3. Ollama starts automatically after install. Verify it is running:

```bash
curl http://localhost:11434
# Expected: "Ollama is running"
```

### Connecting to Ollama

By default the agents connect to `http://localhost:11434`. If your Ollama instance runs on a different machine (e.g. a shared GPU tower on your local network), set the `OLLAMA_HOST` variable in your `.env` file:

```env
OLLAMA_HOST=http://<ip-address>:11434
```

The agents read this at startup via `os.getenv("OLLAMA_HOST", "http://localhost:11434")`, so no code changes are needed.

### Changing the Model

To swap to a different Ollama model, edit the `_get_llm()` function in both agent files:

- `agents/architect.py`
- `agents/engineer.py`

```python
return ChatOllama(
    model="your-model-name",   # ← change this
    base_url=base_url,
    temperature=0.1,
)
```

Any model available in your Ollama library works. Smaller models (`llama3.2:1b`, `phi3`) are faster but produce less reliable transformation code. Larger models (`qwen2.5:14b`, `llama3.1:70b`) handle complex schemas better.

---

## Setup

### Requirements

- Python 3.11+
- Ollama (see above)

### Install

```bash
git clone <repo-url>
cd ETL_Agent

pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:

```env
# Required if Ollama is not on localhost
OLLAMA_HOST=http://localhost:11434

# Required only for Alpha Vantage API source
ALPHA_VANTAGE_API_KEY=your_key_here

# Optional — override backend host/port
HOST=0.0.0.0
PORT=5000

# Optional — override backend URL for the Streamlit frontend
BACKEND_URL=http://localhost:5000
```

---

## Running the Project

There are three ways to run DataWeave depending on what you need.

### 1. Pipeline only (command line)

Runs a built-in CSV demo end-to-end:

```bash
python main.py
```

This creates a sample CSV, runs all four agents, prints the transformation diff, and writes results to `/tmp/demo_etl_output.csv`.

To run against your own file, call `run_pipeline()` directly from a script:

```python
from main import run_pipeline

final = run_pipeline(
    source_type="csv",
    source_config={"path": "datasets/your_file.csv"},
    target_path="output/result.csv",
    user_instructions="Drop rows where rating is null. Normalize date columns to ISO 8601.",
)
```

### 2. Backend API (Flask)

The REST API exposes the pipeline over HTTP and is required for the Streamlit frontend.

```bash
python backend/api.py
```

Server starts at `http://localhost:5000`. Key endpoints:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Check server is running |
| `POST` | `/api/upload` | Upload a CSV file |
| `POST` | `/api/run` | Run the full pipeline (blocking) |
| `POST` | `/api/run/stream` | Run with SSE token streaming |
| `GET` | `/api/results` | Query transformed rows from SQLite |
| `GET` | `/api/schema` | Get output table schema |
| `GET` | `/api/status` | Status of last pipeline run |
| `GET` | `/api/analytics/<query>` | Run a pre-built analytics query |
| `POST` | `/api/query` | Run a custom SELECT query |

#### Example: run the pipeline via API

```bash
# Upload a CSV
curl -X POST http://localhost:5000/api/upload \
  -F "file=@datasets/coursea_data.csv"

# Run the pipeline with custom instructions
curl -X POST http://localhost:5000/api/run \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "csv",
    "source_path": "datasets/coursea_data.csv",
    "user_instructions": "Drop rows where rating is null",
    "target_db_path": "output/etl_output.db",
    "target_table": "courses"
  }'
```

#### Example: stream agent tokens (SSE)

```bash
curl -N -X POST http://localhost:5000/api/run/stream \
  -H "Content-Type: application/json" \
  -d '{"source_type": "csv", "source_path": "datasets/coursea_data.csv"}'
```

Events arrive as `data: {...}\n\n`. Event types:
- `token` — live LLM token from Architect or Engineer
- `node_done` — an agent has finished, includes its output
- `done` — pipeline complete
- `error` — something failed

### 3. Streamlit Frontend

The visual UI. Requires the backend API to be running first.

```bash
# Terminal 1 — start the backend
python backend/api.py

# Terminal 2 — start the frontend
streamlit run frontend/app_t2.2.py
```

Opens at `http://localhost:8501`. The four-step flow:

1. **Upload** — upload a CSV, paste an API URL, or link Google Drive
2. **Mapper** — schema inference, column profiling, ERD diagram, data dictionary
3. **Transform** — type your transformation instructions, click **Run pipeline**, watch agents stream live
4. **Logs & Downloads** — export transformed dataset and data dictionary as CSV

---

## Data Sources

### CSV

```python
run_pipeline(
    source_type="csv",
    source_config={"path": "path/to/file.csv"},
    ...
)
```

### Alpha Vantage API (stock/financial data)

Requires `ALPHA_VANTAGE_API_KEY` in `.env`.

```python
run_pipeline(
    source_type="api",
    source_config={
        "symbol": "AAPL",
        "interval": "Daily",
        "apikey": os.getenv("ALPHA_VANTAGE_API_KEY"),
    },
    ...
)
```

---

## Output Targets

| Target | Config |
|--------|--------|
| CSV file | `target_path="output/result.csv"` |
| JSON file | `target_path="output/result.json"` |
| SQLite | `target_db={"type": "sqlite", "path": "output/result.db", "table": "my_table"}` |
| PostgreSQL | `target_db={"type": "postgres", "connection_string": "postgresql://user:pass@host/db", "table": "my_table"}` |

---

## Project Structure

```
ETL_Agent/
├── main.py                  # Entry point — run_pipeline()
├── script.py                # Quick run script for dev/testing
├── requirements.txt
│
├── agents/
│   ├── scout.py             # Extract + schema inference (no LLM)
│   ├── architect.py         # Transformation planning (LLM)
│   ├── engineer.py          # Code generation + execution (LLM)
│   └── loader.py            # Write to file or database (no LLM)
│
├── pipeline/
│   ├── graph.py             # LangGraph StateGraph assembly
│   ├── etl_state.py         # ETLState TypedDict
│   └── router.py            # Post-engineer conditional router
│
├── tools/
│   ├── api_tools.py         # Alpha Vantage API wrapper
│   └── csv_tools.py         # CSV reader + schema inferrer
│
├── backend/
│   └── api.py               # Flask REST API + SSE streaming
│
├── frontend/
│   └── app_t2.2.py          # Streamlit UI (4-step pipeline)
│
└── datasets/
    └── coursea_data.csv     # Sample dataset (891 Coursera courses)
```

---

## Capstone — S2026A Team 2
