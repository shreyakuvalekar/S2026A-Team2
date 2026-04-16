"""Engineer agent — LLM code generation + execution (US2, US4, US5)."""
import json
import os
import traceback
from datetime import datetime

from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage, SystemMessage


SYSTEM_PROMPT = """/no_think
You are the Engineer agent in a 4-agent ETL pipeline.
Your job is to write Python code that transforms raw_data according to the Architect's plan.

Rules:
- `raw_data` is ALREADY defined in the execution context as a list of dicts. DO NOT redefine it, reassign it, or copy its values into your code as a literal. Just use it directly.
- `pd` (pandas) and `json` are already imported. Do not import anything else.
- Your code MUST assign the final result to a variable called `result`.
- `result` must be a list of dicts (use df.to_dict(orient='records') if you use pandas).
- Write ONLY executable Python code. No markdown fences, no explanations, no comments.
- CRITICAL: If the plan contains a "Filter rows" step, implement it as a row-selection filter (e.g. df = df[df['col'].str.contains('value', case=False, na=False)]). The result must contain ONLY the matching rows.
- Only fall back to `result = raw_data` if the plan truly has no transformations to apply."""

USER_PROMPT_TEMPLATE = """TRANSFORMATION PLAN (implement every step exactly):
{plan}

Column schema:
{schema}

Sample rows (5 rows for column reference only — raw_data already holds the full dataset at runtime, do NOT hardcode these values):
{sample}

Write Python code that implements ONLY the steps in the TRANSFORMATION PLAN above.
- Use `raw_data` as the input variable (already defined).
- Assign the final output to `result` as a list of dicts.
- No imports, no markdown, no comments. Start directly with code."""


def _get_llm() -> ChatOllama:
    base_url = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    return ChatOllama(
        model="qwen2.5:14b-instruct-q4_K_M",
        base_url=base_url,
        temperature=0.0,
    )


def _compute_diff(raw_data: list, transformed_data: list) -> dict:
    """Compute what changed between raw_data and transformed_data.

    Uses SequenceMatcher (LCS) to align rows so that:
    - 'delete' opcodes → truly removed rows (never modified)
    - 'replace' opcodes → modified rows (value changes); extra raw rows in an
      unequal-length replace block are treated as removed
    - 'equal' opcodes → unchanged rows
    """
    from difflib import SequenceMatcher

    raw_keys = [json.dumps(r, sort_keys=True, default=str) for r in raw_data]
    tr_keys = [json.dumps(r, sort_keys=True, default=str) for r in transformed_data]

    matcher = SequenceMatcher(None, raw_keys, tr_keys, autojunk=False)

    removed_rows = []
    modified_rows = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "delete":
            removed_rows.extend(raw_data[i1:i2])
        elif tag == "replace":
            raw_block = raw_data[i1:i2]
            tr_block = transformed_data[j1:j2]
            for raw_row, tr_row in zip(raw_block, tr_block):
                changed_fields = [
                    k for k in set(raw_row) | set(tr_row)
                    if raw_row.get(k) != tr_row.get(k)
                ]
                modified_rows.append({
                    "before": raw_row,
                    "after": tr_row,
                    "changed_fields": changed_fields,
                })
            # Any extra raw rows in the block have no transformed counterpart
            if len(raw_block) > len(tr_block):
                removed_rows.extend(raw_block[len(tr_block):])

    raw_cols = set(raw_data[0].keys()) if raw_data else set()
    tr_cols = set(transformed_data[0].keys()) if transformed_data else set()

    return {
        "rows_before": len(raw_data),
        "rows_after": len(transformed_data),
        "rows_removed_count": len(removed_rows),
        "removed_rows": removed_rows[:100],
        "modified_rows": modified_rows[:100],
        "columns_before": list(raw_cols),
        "columns_after": list(tr_cols),
        "columns_dropped": list(raw_cols - tr_cols),
        "columns_added": list(tr_cols - raw_cols),
    }


def _extract_code(text: str) -> str:
    """Strip markdown code fences and Qwen3 <think> blocks if present."""
    import re
    # Strip <think>...</think> blocks (Qwen3 thinking mode leakage)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    lines = text.strip().splitlines()
    # Remove ```python / ``` wrappers
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines)


def engineer_generate_node(state: dict) -> dict:
    """LLM step: generate transformation code and save it to a file."""
    raw_data = state.get("raw_data", [])
    raw_schema = state.get("raw_schema", {})
    transformation_plan = state.get("transformation_plan", "")
    audit_log = list(state.get("audit_log", []))

    user_msg = USER_PROMPT_TEMPLATE.format(
        plan=transformation_plan,
        schema=json.dumps(raw_schema, indent=2),
        sample=json.dumps(raw_data[:5], indent=2),
    )

    llm = _get_llm()
    full_content = ""
    for chunk in llm.stream([
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=user_msg),
    ]):
        full_content += chunk.content or ""
    transformation_code = _extract_code(full_content.strip())

    # Save to a timestamped file so every run is inspectable / diffable
    output_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "engineer_outputs")
    )
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    generated_code_path = os.path.join(output_dir, f"engineer_output_{ts}.py")
    with open(generated_code_path, "w") as f:
        f.write(transformation_code)

    audit_log.append({
        "timestamp": datetime.utcnow().isoformat(),
        "agent": "Engineer",
        "action": "generate",
        "summary": f"Code generated and saved to {generated_code_path}",
    })

    return {
        "transformation_code": transformation_code,
        "generated_code_path": generated_code_path,
        "audit_log": audit_log,
    }


def engineer_execute_node(state: dict) -> dict:
    """Deterministic step: execute the generated code — no LLM involved."""
    raw_data = state.get("raw_data", [])
    transformation_code = state.get("transformation_code", "")
    retry_count = state.get("retry_count", 0)
    max_retries = state.get("max_retries", 3)
    audit_log = list(state.get("audit_log", []))

    import pandas as pd  # noqa: F401 — available in exec namespace

    # Provide raw_data under every plausible name the LLM might use
    namespace = {
        "raw_data": raw_data,
        "data": raw_data,
        "records": raw_data,
        "rows": raw_data,
        "courses": raw_data,
        "dataset": raw_data,
        "pd": pd,
        "json": json,
        "result": None,
    }

    verdict = "pass"
    transformed_data = None
    transformation_diff = {}
    error_msg = ""

    try:
        exec(transformation_code, namespace)  # noqa: S102
        transformed_data = namespace.get("result")
        if transformed_data is None:
            raise ValueError("Code executed but 'result' was never assigned.")
        if not isinstance(transformed_data, list):
            raise TypeError(f"'result' must be a list of dicts, got {type(transformed_data).__name__}")

        transformation_diff = _compute_diff(raw_data, transformed_data)

        audit_log.append({
            "timestamp": datetime.utcnow().isoformat(),
            "agent": "Engineer",
            "action": "execute",
            "summary": (
                f"Code executed successfully. {len(transformed_data)} records produced. "
                f"{transformation_diff['rows_removed_count']} rows removed, "
                f"{len(transformation_diff['modified_rows'])} rows modified."
            ),
        })

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
        verdict = "retry" if retry_count < max_retries else "escalate"

        audit_log.append({
            "timestamp": datetime.utcnow().isoformat(),
            "agent": "Engineer",
            "action": "execute_error",
            "summary": f"Execution failed (retry {retry_count}/{max_retries}): {exc}",
        })

    return {
        "transformed_data": transformed_data,
        "transformation_diff": transformation_diff,
        "engineer_verdict": verdict,
        "engineer_error": error_msg,
        "retry_count": retry_count + (1 if verdict == "retry" else 0),
        "audit_log": audit_log,
    }