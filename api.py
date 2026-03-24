"""
DataWeave Backend API (Flask)

This service exposes REST endpoints for:
- Uploading CSV datasets
- Running the team ETL pipeline
- Querying transformed data from SQLite
- Retrieving schema metadata
- Running pre-built analytics queries
- Executing safe custom SELECT queries

Architecture:
Client (Jupyter / UI) -> Flask API -> Team ETL Pipeline -> SQLite DB

Run:
    python api.py

Then open:
    http://localhost:5000/health
"""

import os
import sqlite3
import traceback

import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS

from main import run_pipeline  # Team pipeline entry point

load_dotenv()

app = Flask(__name__)
CORS(app)

# In-memory state for the current app session
_last_run = {}
_uploaded_file_path = None


# --------------------------------------------------
# Health Check Endpoint
# --------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    """
    Verify that the backend server is running.

    Returns:
        JSON response with service status and project metadata.
    """
    return jsonify({
        "status": "ok",
        "project": "DataWeave",
        "team": "S2026A-Team2"
    })


# --------------------------------------------------
# 1. Upload CSV
# --------------------------------------------------

@app.route("/api/upload", methods=["POST"])
def upload_csv():
    """
    Upload a CSV file and generate a quick column profile.

    Returns:
        - upload status
        - saved file path
        - row count
        - column count
        - per-column metadata:
            * dtype
            * null count
            * unique count
            * sample values
            * numeric summary (min, max, mean) when applicable
    """
    global _uploaded_file_path

    if "file" not in request.files:
        return jsonify({
            "error": "No file provided. Send a CSV as form field 'file'."
        }), 400

    uploaded = request.files["file"]

    if not uploaded.filename or not uploaded.filename.endswith(".csv"):
        return jsonify({
            "error": "Only .csv files are accepted."
        }), 400

    save_path = os.path.join("datasets", uploaded.filename)
    os.makedirs("datasets", exist_ok=True)
    uploaded.save(save_path)
    _uploaded_file_path = save_path

    try:
        df = pd.read_csv(save_path)

        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        columns = []
        for col in df.columns:
            col_info = {
                "name": col,
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "unique_count": int(df[col].nunique()),
                "sample_values": df[col].dropna().head(5).tolist(),
            }

            if pd.api.types.is_numeric_dtype(df[col]):
                col_info["min"] = float(df[col].min())
                col_info["max"] = float(df[col].max())
                col_info["mean"] = round(float(df[col].mean()), 4)

            columns.append(col_info)

        return jsonify({
            "status": "uploaded",
            "filename": uploaded.filename,
            "saved_path": save_path,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": columns,
        })

    except Exception as e:
        return jsonify({
            "error": f"Failed to read CSV: {str(e)}"
        }), 422


# --------------------------------------------------
# 2. Run ETL Pipeline
# --------------------------------------------------

@app.route("/api/run", methods=["POST"])
def run_pipeline_endpoint():
    """
    Run the full ETL pipeline using the uploaded CSV or a default dataset.

    Request body (optional JSON):
        {
            "source_path": "datasets/coursea_data.csv",
            "target_db_path": "output/etl_output.db",
            "target_table": "courses",
            "if_exists": "replace",
            "user_instructions": "custom transformation instructions"
        }

    Notes:
        - If no source_path is provided, the API uses the most recently uploaded CSV.
        - If no uploaded file exists, it falls back to datasets/coursea_data.csv.
        - This endpoint depends on the team's pipeline implementation from main.py.
        - If the LLM/Ollama layer is unavailable or system resources are low,
          pipeline execution may fail before completion.
    """
    global _last_run

    body = request.get_json(silent=True) or {}

    source_type = body.get("source_type", "csv")
    connection_port = body.get("connection_port")
    user_instructions = body.get("user_instructions", "")

    target_db_path = body.get("target_db_path", "output/etl_output.db")
    target_table = body.get("target_table", "courses")
    if_exists = body.get("if_exists", "replace")
    target_path = body.get("target_path", "")

    if source_type == "csv":
        source_path = body.get("source_path") or _uploaded_file_path or "datasets/coursea_data.csv"
        source_config = {"path": source_path}

    elif source_type == "api":
        source_config = {
            "symbol": body.get("symbol"),
            "interval": body.get("interval", "Daily"),
            "apikey": body.get("apikey"),
        }

        missing = [k for k, v in source_config.items() if not v]
        if missing:
            return jsonify({
                "status": "error",
                "error": f"Missing required API source fields: {missing}"
            }), 400
        source_path = None

    else:
        return jsonify({
            "status": "error",
            "error": f"Unsupported source_type '{source_type}'. Use 'csv' or 'api'."
        }), 400

    os.makedirs("output", exist_ok=True)

    try:
        final_state = run_pipeline(
            source_type=source_type,
            source_config=source_config,
            target_path=target_path,
            target_db={
                "type": "sqlite",
                "path": target_db_path,
                "table": target_table,
                "if_exists": if_exists,
            },
            user_instructions=user_instructions,
            connection_port=connection_port,
        )

        rows_written = _count_rows(target_db_path, target_table)

        result = {
            "status": "success",
            "source_type": source_type,
            "source_path": source_path,
            "target_db_path": target_db_path,
            "target_table": target_table,
            "rows_written": rows_written,
            "agent_outputs": {
                "scout": {
                    "raw_schema": final_state.get("raw_schema", {}),
                    "record_count": len(final_state.get("raw_data") or []),
                    "sample_records": (final_state.get("raw_data") or [])[:5],
                },
                "architect": {
                    "transformation_plan": final_state.get("transformation_plan", ""),
                },
                "engineer": {
                    "transformation_code": final_state.get("transformation_code", ""),
                    "verdict": final_state.get("engineer_verdict", ""),
                    "error": final_state.get("engineer_error", ""),
                    "output_record_count": len(
                        final_state.get("transformed_data") or []
                    ),
                    "sample_output": (final_state.get("transformed_data") or [])[:5],
                },
            },
            "audit_log": final_state.get("audit_log", []),
        }

        _last_run = result
        return jsonify(result)

    except Exception as e:
        error_result = {
            "status": "error",
            "source_type": source_type,
            "connection_port": connection_port,
            "error": str(e),
            "traceback": traceback.format_exc(),
        }
        _last_run = error_result
        return jsonify(error_result), 500

# --------------------------------------------------
# 3. Get Results from SQLite
# --------------------------------------------------

@app.route("/api/results", methods=["GET"])
def get_results():
    """
    Query the output SQLite database and return rows as JSON.

    Query parameters:
        db_path (default: output/etl_output.db)
        table   (default: courses)
        limit   (default: 100)
        offset  (default: 0)

    Returns:
        - table name
        - total row count
        - returned row count
        - selected rows
        - column names
    """
    db_path = request.args.get("db_path", "output/etl_output.db")
    table = request.args.get("table", "courses")
    limit = int(request.args.get("limit", 100))
    offset = int(request.args.get("offset", 0))

    if not os.path.exists(db_path):
        return jsonify({
            "error": f"Database not found at '{db_path}'. Run /api/run first."
        }), 404

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            f"SELECT * FROM {table} LIMIT ? OFFSET ?",
            (limit, offset)
        )
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        conn.close()

        return jsonify({
            "table": table,
            "total_rows": total,
            "returned": len(rows),
            "limit": limit,
            "offset": offset,
            "columns": columns,
            "rows": rows,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --------------------------------------------------
# 4. Get Schema / Column Info
# --------------------------------------------------

@app.route("/api/schema", methods=["GET"])
def get_schema():
    """
    Return the schema metadata of the output SQLite table.

    Query parameters:
        db_path (default: output/etl_output.db)
        table   (default: courses)

    Returns:
        - table name
        - row count
        - column-level schema information
    """
    db_path = request.args.get("db_path", "output/etl_output.db")
    table = request.args.get("table", "courses")

    if not os.path.exists(db_path):
        return jsonify({
            "error": f"Database not found at '{db_path}'. Run /api/run first."
        }), 404

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(f"PRAGMA table_info({table})")
        columns = [
            {
                "cid": row[0],
                "name": row[1],
                "type": row[2],
                "notnull": bool(row[3]),
                "pk": bool(row[5]),
            }
            for row in cursor.fetchall()
        ]
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        conn.close()

        return jsonify({
            "table": table,
            "row_count": row_count,
            "columns": columns,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --------------------------------------------------
# 5. Pre-Built Analytics Queries
# --------------------------------------------------

@app.route("/api/analytics/<query_name>", methods=["GET"])
def analytics(query_name):
    """
    Execute a predefined analytics query against the output database.

    Available query names:
        - avg_rating_by_difficulty
        - top_organizations
        - certificate_distribution
        - rating_distribution
        - enrollment_by_difficulty

    Query parameters:
        db_path (default: output/etl_output.db)
    """
    db_path = request.args.get("db_path", "output/etl_output.db")

    queries = {
        "avg_rating_by_difficulty": """
            SELECT course_difficulty,
                   ROUND(AVG(course_rating), 3) AS avg_rating,
                   COUNT(*) AS course_count
            FROM courses
            GROUP BY course_difficulty
            ORDER BY avg_rating DESC
        """,
        "top_organizations": """
            SELECT course_organization,
                   COUNT(*) AS num_courses,
                   ROUND(AVG(course_rating), 3) AS avg_rating
            FROM courses
            GROUP BY course_organization
            ORDER BY num_courses DESC
            LIMIT 15
        """,
        "certificate_distribution": """
            SELECT course_Certificate_type AS certificate_type,
                   COUNT(*) AS count
            FROM courses
            GROUP BY course_Certificate_type
            ORDER BY count DESC
        """,
        "rating_distribution": """
            SELECT ROUND(course_rating, 1) AS rating_bucket,
                   COUNT(*) AS count
            FROM courses
            WHERE course_rating IS NOT NULL
            GROUP BY rating_bucket
            ORDER BY rating_bucket
        """,
        "enrollment_by_difficulty": """
            SELECT course_difficulty,
                   ROUND(AVG(course_students_enrolled), 0) AS avg_enrollment,
                   COUNT(*) AS course_count
            FROM courses
            GROUP BY course_difficulty
            ORDER BY avg_enrollment DESC
        """,
    }

    if query_name not in queries:
        return jsonify({
            "error": f"Unknown query '{query_name}'.",
            "available": list(queries.keys())
        }), 400

    if not os.path.exists(db_path):
        return jsonify({
            "error": f"Database not found at '{db_path}'. Run /api/run first."
        }), 404

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(queries[query_name])
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()

        return jsonify({
            "query": query_name,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --------------------------------------------------
# 6. Custom SQL Query
# --------------------------------------------------

@app.route("/api/query", methods=["POST"])
def custom_query():
    """
    Execute a custom read-only SQL query against the SQLite output DB.

    Request body:
        {
            "sql": "SELECT * FROM courses LIMIT 10",
            "db_path": "output/etl_output.db"
        }

    Safety:
        Only SELECT statements are allowed.
    """
    body = request.get_json(silent=True) or {}
    sql = body.get("sql", "").strip()
    db_path = body.get("db_path", "output/etl_output.db")

    if not sql:
        return jsonify({
            "error": "Provide a 'sql' field in the request body."
        }), 400

    if not sql.lower().startswith("select"):
        return jsonify({
            "error": "Only SELECT statements are allowed."
        }), 400

    if not os.path.exists(db_path):
        return jsonify({
            "error": f"Database not found at '{db_path}'."
        }), 404

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()

        return jsonify({
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --------------------------------------------------
# 7. Pipeline Status
# --------------------------------------------------

@app.route("/api/status", methods=["GET"])
def status():
    """
    Return the status of the most recent pipeline execution.

    Returns:
        - success/error/never_run
        - rows written
        - engineer verdict
        - error message if any
    """
    if not _last_run:
        return jsonify({
            "status": "never_run",
            "message": "No pipeline run yet. POST to /api/run."
        })
    return jsonify(_last_run)


# --------------------------------------------------
# Helper Functions
# --------------------------------------------------

def _count_rows(db_path: str, table: str):
    """
    Return the row count of a SQLite table.

    Returns:
        int row count if successful, otherwise None.
    """
    try:
        conn = sqlite3.connect(db_path)
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        conn.close()
        return count
    except Exception:
        return None


# --------------------------------------------------
# Entry Point
# --------------------------------------------------

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "5000"))

    print("=" * 50)
    print("  DataWeave Backend API")
    print(f"  http://{host}:{port}/health")
    print("=" * 50)

    app.run(host=host, port=port, debug=False)