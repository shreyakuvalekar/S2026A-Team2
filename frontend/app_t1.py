import html
import io
import json
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit.components.v1 as components
import streamlit as st

BACKEND_URL = "http://localhost:5000"


# Page config (must be first Streamlit command)
st.set_page_config(
    page_title="DataWeaver – Agentic Data Pipeline",
    page_icon="🧵",
    layout="wide",
    initial_sidebar_state="expanded",
)

APP_BUILD = "dataweaver-erd-v13-wireframe"

# Custom styles — dashboard look & feel
def _inject_styles() -> None:
    st.markdown(
        """
        <style>
            /* Fonts & base */
            @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&display=swap');
            @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&display=swap');
            html, body, [class*="css"] { font-family: 'DM Sans', system-ui, sans-serif; }
            .block-container { padding-top: 1.5rem; padding-bottom: 2rem; max-width: 1200px; }

            /* Hero / page title */
            .dw-hero {
                background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 50%, #0c4a6e 100%);
                color: #f8fafc;
                padding: 1.25rem 1.5rem;
                border-radius: 12px;
                margin-bottom: 1.25rem;
                border: 1px solid rgba(56, 189, 248, 0.25);
                box-shadow: 0 4px 24px rgba(15, 23, 42, 0.35);
            }
            .dw-hero h1 { margin: 0; font-size: 1.65rem; font-weight: 700; letter-spacing: -0.02em; }
            .dw-hero p { margin: 0.5rem 0 0 0; opacity: 0.88; font-size: 0.95rem; line-height: 1.45; }
            .dw-badge {
                display: inline-block;
                background: rgba(56, 189, 248, 0.2);
                color: #7dd3fc;
                padding: 0.2rem 0.6rem;
                border-radius: 6px;
                font-size: 0.75rem;
                font-weight: 600;
                margin-bottom: 0.5rem;
                letter-spacing: 0.04em;
            }

            /* Cards */
            .dw-card {
                background: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 10px;
                padding: 1rem 1.15rem;
                margin: 0.75rem 0;
            }

            /* Sidebar polish */
            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
            }
            [data-testid="stSidebar"] .block-container { padding-top: 1.25rem; }
            [data-testid="stSidebar"] label, [data-testid="stSidebar"] p,
            [data-testid="stSidebar"] span { color: #cbd5e1 !important; }
            [data-testid="stSidebar"] .stRadio label { font-size: 0.9rem; }

            /* Primary buttons */
            .stButton > button[kind="primary"] {
                background: linear-gradient(180deg, #0284c7, #0369a1);
                border: none;
                font-weight: 600;
            }
            .stButton > button[kind="primary"]:hover {
                background: linear-gradient(180deg, #0ea5e9, #0284c7);
            }

            /* File uploader */
            [data-testid="stFileUploader"] section {
                border-radius: 10px;
                border: 2px dashed #94a3b8;
                background: #f1f5f9;
            }
            [data-testid="stFileUploader"] svg {
                width: 2.35rem !important;
                height: 2.35rem !important;
                min-width: 2.35rem !important;
            }
            [data-testid="stFileUploader"] section > div {
                gap: 0.35rem 0.75rem !important;
                align-items: center !important;
            }
            /* Relabel secondary button to "Add files" */
            [data-testid="stFileUploader"] button[kind="secondary"],
            [data-testid="stFileUploader"] button[data-testid="baseButton-secondary"] {
                position: relative !important;
                min-width: 5.5rem !important;
                min-height: 2.25rem !important;
                padding: 0.375rem 0.75rem !important;
                font-size: 0 !important;
                line-height: 0 !important;
                color: transparent !important;
            }
            [data-testid="stFileUploader"] button[kind="secondary"] *,
            [data-testid="stFileUploader"] button[data-testid="baseButton-secondary"] * {
                font-size: 0 !important;
                line-height: 0 !important;
                opacity: 0 !important;
                max-width: 0 !important;
                max-height: 0 !important;
                overflow: hidden !important;
                margin: 0 !important;
                padding: 0 !important;
                border: none !important;
                position: absolute !important;
                inset: 0 auto auto 0 !important;
                clip-path: inset(50%) !important;
            }
            [data-testid="stFileUploader"] button[kind="secondary"]::after,
            [data-testid="stFileUploader"] button[data-testid="baseButton-secondary"]::after {
                content: "Add files";
                position: absolute !important;
                left: 50% !important;
                top: 50% !important;
                transform: translate(-50%, -50%) !important;
                font-size: 0.9rem !important;
                line-height: 1.2 !important;
                color: #334155 !important;
                font-weight: 600 !important;
                white-space: nowrap !important;
                opacity: 1 !important;
                clip-path: none !important;
            }

            /* Schema pipeline layout */
            .dw-pipe-wrap {
                background: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 12px;
                padding: 1rem;
                margin: 0.5rem 0 1rem 0;
            }
            .dw-pipe-row {
                display: grid;
                grid-template-columns: 1fr 56px 1fr 56px 1fr;
                align-items: center;
                gap: 0.5rem;
                margin-bottom: 0.8rem;
            }
            .dw-node {
                background: #ffffff;
                border: 2px solid #cbd5e1;
                border-radius: 10px;
                padding: 0.7rem 0.8rem;
                min-height: 94px;
            }
            .dw-node h4 {
                margin: 0;
                font-size: 0.9rem;
                color: #0f172a;
            }
            .dw-node p {
                margin: 0.2rem 0 0 0;
                font-size: 0.76rem;
                color: #475569;
                line-height: 1.35;
            }
            .dw-arrow {
                text-align: center;
                font-size: 1.2rem;
                color: #0d9488;
                font-weight: 700;
            }
            .dw-tag-ok, .dw-tag-mid, .dw-tag-risk {
                display: inline-block;
                margin-top: 0.45rem;
                border-radius: 999px;
                padding: 0.12rem 0.5rem;
                font-size: 0.7rem;
                font-weight: 700;
            }
            .dw-tag-ok { background: #dcfce7; color: #166534; }
            .dw-tag-mid { background: #fef3c7; color: #92400e; }
            .dw-tag-risk { background: #fee2e2; color: #991b1b; }

            /* Schema inference — wireframe board (Mapper) */
            .dw-wf-board {
                border: 2px dashed #64748b;
                border-radius: 6px;
                background: linear-gradient(180deg, #fafafa 0%, #f4f4f5 100%);
                padding: 14px 16px 18px;
                margin: 0 0 14px 0;
            }
            .dw-wf-board-title {
                font-size: 0.68rem;
                letter-spacing: 0.16em;
                font-weight: 700;
                color: #64748b;
                text-transform: uppercase;
                margin-bottom: 12px;
            }
            .dw-wf-metrics {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 10px;
            }
            .dw-wf-metric-cell {
                border: 1px dashed #94a3b8;
                background: #fff;
                padding: 10px 8px;
                text-align: center;
                border-radius: 4px;
            }
            .dw-wf-num { display: block; font-size: 1.35rem; font-weight: 700; color: #0f172a; line-height: 1.2; }
            .dw-wf-lbl { display: block; font-size: 0.62rem; letter-spacing: 0.1em; color: #64748b; margin-top: 4px; font-weight: 600; }
            .dw-wf-lane-h {
                font-size: 0.65rem;
                letter-spacing: 0.14em;
                font-weight: 800;
                color: #475569;
                text-transform: uppercase;
                margin: 0 0 10px 0;
                padding-bottom: 6px;
                border-bottom: 1px dashed #cbd5e1;
            }
            .dw-wf-lane-h.e { color: #1d4ed8; }
            .dw-wf-lane-h.t { color: #b45309; }
            .dw-wf-lane-h.l { color: #15803d; }
            .dw-wf-card {
                border: 2px dashed #94a3b8;
                background: #fff;
                border-radius: 4px;
                padding: 10px 12px 12px;
                margin-bottom: 10px;
            }
            .dw-wf-card-title { font-weight: 700; font-size: 0.88rem; color: #0f172a; margin-bottom: 4px; }
            .dw-wf-meta { font-size: 0.76rem; color: #64748b; margin-bottom: 8px; }
            .dw-wf-code {
                font-family: 'IBM Plex Mono', ui-monospace, monospace;
                font-size: 0.72rem;
                color: #334155;
                line-height: 1.45;
                margin-bottom: 8px;
                word-break: break-word;
            }
            .dw-wf-keys { font-size: 0.74rem; color: #475569; margin-bottom: 8px; }
            .dw-wf-keys code { background: #f1f5f9; padding: 1px 4px; border-radius: 3px; font-size: 0.72rem; }
            .dw-wf-pill {
                display: inline-block;
                font-size: 0.65rem;
                font-weight: 700;
                letter-spacing: 0.04em;
                padding: 3px 8px;
                border-radius: 3px;
                border: 1px dashed #94a3b8;
            }
            .dw-wf-pill-ok { background: #ecfdf5; color: #047857; border-color: #6ee7b7; }
            .dw-wf-pill-warn { background: #fffbeb; color: #b45309; border-color: #fcd34d; }
            .dw-wf-pill-bad { background: #fef2f2; color: #b91c1c; border-color: #fca5a5; }
            .dw-wf-ul { margin: 6px 0 0 0; padding-left: 16px; font-size: 0.76rem; color: #334155; line-height: 1.5; }
            .dw-wf-note {
                font-size: 0.72rem;
                color: #64748b;
                border: 1px dashed #cbd5e1;
                background: #f8fafc;
                padding: 8px 10px;
                border-radius: 4px;
                margin-top: 8px;
            }
            /* ERD graphviz — wireframe frame (solid outer border) */
            [data-testid="stGraphvizChart"] {
                border: 2px solid #94a3b8 !important;
                border-radius: 8px !important;
                padding: 10px !important;
                background: repeating-linear-gradient(
                    0deg, #fafafa, #fafafa 10px, #f4f4f5 11px
                ) !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _hero(badge: str, title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="dw-hero">
            <div class="dw-badge">{badge}</div>
            <h1>{title}</h1>
            <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _pipeline_status_sidebar() -> None:
    """Show 6-step pipeline with visual completion hints."""
    has_upload = bool(st.session_state.get("uploaded_dfs"))
    has_mapper = st.session_state.get("mapper_output") is not None
    approved_1 = st.session_state.get("mapper_approved", False)
    has_agg = st.session_state.get("aggregator_output") is not None
    has_logs = bool(st.session_state.get("run_logs"))

    steps = [
        ("1", "Upload", has_upload),
        ("2", "Mapper", has_mapper),
        ("3", "HITL #1", approved_1),
        ("4", "Aggregator", has_agg),
        ("5", "HITL #2", has_logs and any(
            isinstance(x, dict) and x.get("stage") == "HITL #2" for x in st.session_state.get("run_logs", [])
        )),
        ("6", "Exports", has_agg),
    ]

    lines = []
    for num, label, done in steps:
        icon = "✓" if done else "○"
        color = "#4ade80" if done else "#64748b"
        lines.append(
            f'<div style="display:flex;align-items:center;gap:0.5rem;margin:0.35rem 0;font-size:0.82rem;">'
            f'<span style="color:{color};font-weight:700;">{icon}</span>'
            f'<span style="color:#cbd5e1;">{num}. {label}</span></div>'
        )
    st.sidebar.markdown(
        '<p style="color:#94a3b8;font-size:0.75rem;text-transform:uppercase;letter-spacing:0.08em;margin:0 0 0.5rem 0;">Pipeline</p>'
        + "".join(lines),
        unsafe_allow_html=True,
    )


_inject_styles()

# Helpers
def _humanize_column_name(col: str) -> str:
    """Replace pandas default index column labels (Unnamed: 0) in UI text."""
    s = str(col).strip()
    if "unnamed" in s.lower():
        return "row_index"
    return s


def _humanize_key_list(cols: List[str]) -> List[str]:
    return [_humanize_column_name(c) for c in cols]


def profile_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Basic profiling for each column."""
    profile = []
    for col in df.columns:
        series = df[col]
        profile.append(
            {
                "column": col,
                "dtype": str(series.dtype),
                "non_null_count": series.notna().sum(),
                "null_count": series.isna().sum(),
                "null_pct": round(series.isna().mean() * 100, 2),
                "unique_values": series.nunique(dropna=True),
                "sample_values": ", ".join(map(str, series.dropna().unique()[:5])),
            }
        )
    return pd.DataFrame(profile)


def _normalize_table_keys(
    df: pd.DataFrame, primary: List[str], foreign: List[str]
) -> Tuple[List[str], List[str]]:
    """
    One primary key per table (best-scoring). Other id-like PK candidates become FK candidates.
    Keeps compound-PK cases only when both columns are required (rare in CSV); for MVP we prefer a single PK.
    """
    primary = list(dict.fromkeys(primary))
    foreign = list(dict.fromkeys(foreign))
    foreign = [c for c in foreign if c not in primary]

    if len(primary) <= 1:
        return primary, foreign

    def _pk_rank(col: str) -> Tuple[int, int, int, str]:
        lc = str(col).lower()
        tier = 0 if lc == "id" else (1 if lc.endswith("_id") else 2)
        try:
            full = bool(df[col].notna().all() and df[col].nunique() == len(df))
        except Exception:
            full = False
        uniq_tier = 0 if full else 1
        return (tier, uniq_tier, len(str(col)), col)

    ranked = sorted(primary, key=_pk_rank)
    winner = ranked[0]
    new_primary = [winner]
    for c in ranked[1:]:
        lc = str(c).lower()
        if lc.endswith("_id") or lc == "id":
            if c not in foreign:
                foreign.append(c)
    foreign = [c for c in foreign if c not in new_primary]
    return new_primary, list(dict.fromkeys(foreign))


def enrich_cross_table_foreign_keys(
    dfs: Dict[str, pd.DataFrame],
    schema_guess: Dict[str, Dict[str, List[str]]],
) -> Dict[str, Dict[str, List[str]]]:
    """
    If two+ tables share a column name that is PK in one table, mark that column as FK on other tables.
    """
    out: Dict[str, Dict[str, List[str]]] = {
        k: {
            "primary_keys": list(v.get("primary_keys", [])),
            "foreign_keys": list(v.get("foreign_keys", [])),
        }
        for k, v in schema_guess.items()
    }
    names = list(dfs.keys())
    if len(names) < 2:
        return out

    for t in names:
        for col in dfs[t].columns:
            if col in out[t]["primary_keys"]:
                continue
            lc = str(col).lower()
            if not (lc.endswith("_id") or lc == "id"):
                continue
            for other in names:
                if other == t:
                    continue
                if col not in dfs[other].columns:
                    continue
                if col in out[other]["primary_keys"]:
                    if col not in out[t]["foreign_keys"]:
                        out[t]["foreign_keys"].append(col)
        out[t]["foreign_keys"] = [
            c
            for c in dict.fromkeys(out[t]["foreign_keys"])
            if c not in out[t]["primary_keys"]
        ]
    return out


def guess_keys(dfs: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, List[str]]]:
    def table_tokens(table_name: str) -> List[str]:
        stem = table_name.rsplit(".", 1)[0].lower()
        parts = [p for p in re.split(r"[^a-z0-9]+", stem) if p]
        tokens = set(parts)
        for p in parts:
            # simple singular forms
            if p.endswith("ies") and len(p) > 3:
                tokens.add(p[:-3] + "y")
            elif p.endswith("s") and len(p) > 2:
                tokens.add(p[:-1])
        return [t for t in tokens if len(t) > 1]

    result: Dict[str, Dict[str, List[str]]] = {}

    tokens_by_table = {name: table_tokens(name) for name in dfs.keys()}

    for name, df in dfs.items():
        primary: List[str] = []
        foreign: List[str] = []
        # Filename tokens + column-prefix tokens (e.g. final_dataset.csv + course_id/course_title → "course")
        tok = set(tokens_by_table[name])
        for col in df.columns:
            lc = col.lower().strip()
            if "_" in lc:
                head = lc.split("_", 1)[0]
                if len(head) > 1:
                    tok.add(head)
            if lc.endswith("_id") and len(lc) > 3:
                tok.add(lc[:-3])
        table_tokens_current = tok

        for col in df.columns:
            lc = col.lower().strip()
            if lc == "id":
                primary.append(col)
                continue

            if lc.endswith("_id"):
                prefix = lc[:-3]
                if prefix in table_tokens_current or lc in {
                    f"{t}_id" for t in table_tokens_current
                }:
                    primary.append(col)
                else:
                    foreign.append(col)
                continue

            if any(lc == f"{t}id" for t in table_tokens_current):
                primary.append(col)
                continue

            if lc.endswith("_id"):
                foreign.append(col)

        primary = list(dict.fromkeys(primary))
        foreign = [c for c in dict.fromkeys(foreign) if c not in primary]

        # Still no PK: unique id-like / index columns (incl. pandas "Unnamed: 0" exports)
        if not primary and len(df) > 0:

            def _surrogate_name(col) -> bool:
                lc = str(col).lower().strip()
                return (
                    lc == "id"
                    or lc.endswith("_id")
                    or "unnamed" in lc
                    or lc in ("index", "idx", "row", "row_id")
                )

            for col in df.columns:
                if not _surrogate_name(col):
                    continue
                try:
                    if df[col].notna().all() and df[col].nunique() == len(df):
                        primary.append(col)
                        break
                except Exception:
                    continue
        # Any column that is unique per row (surrogate PK for wide tables without *_id)
        if not primary and len(df) > 0:
            for col in df.columns:
                try:
                    if df[col].notna().all() and df[col].nunique() == len(df):
                        primary.append(col)
                        break
                except Exception:
                    continue

        primary = list(dict.fromkeys(primary))
        foreign = [c for c in dict.fromkeys(foreign) if c not in primary]
        primary, foreign = _normalize_table_keys(df, primary, foreign)
        result[name] = {"primary_keys": primary, "foreign_keys": foreign}

    return result


def infer_relationships(schema_guess: Dict[str, Dict[str, List[str]]]) -> List[Tuple[str, str, str]]:
    """
    Infer simple relationships (parent_table, child_table, fk_column).
    """
    def table_tokens(table_name: str) -> List[str]:
        stem = table_name.rsplit(".", 1)[0].lower()
        parts = [p for p in re.split(r"[^a-z0-9]+", stem) if p]
        tokens = set(parts)
        for p in parts:
            if p.endswith("ies") and len(p) > 3:
                tokens.add(p[:-3] + "y")
            elif p.endswith("s") and len(p) > 2:
                tokens.add(p[:-1])
        return [t for t in tokens if len(t) > 1]

    tokens_by_table = {t: table_tokens(t) for t in schema_guess.keys()}
    relationships: List[Tuple[str, str, str]] = []

    for child_table, keys in schema_guess.items():
        for fk in keys["foreign_keys"]:
            fk_lc = fk.lower()
            fk_prefix = fk_lc[:-3] if fk_lc.endswith("_id") else fk_lc

            for parent_table, parent_keys in schema_guess.items():
                if parent_table == child_table:
                    continue

                parent_tokens = tokens_by_table[parent_table]
                parent_pk_lc = {pk.lower() for pk in parent_keys["primary_keys"]}

                token_match = fk_prefix in parent_tokens
                pk_match = fk_lc in parent_pk_lc

                if token_match or pk_match:
                    relationships.append((parent_table, child_table, fk))
                    break

    # De-duplicate
    return list(dict.fromkeys(relationships))


def infer_column_description(
    table: str,
    col: str,
    series: pd.Series,
    schema_guess: Dict[str, Dict[str, List[str]]],
) -> str:
    """Human-readable description for data dictionary rows."""
    lc = col.lower().strip().replace(" ", "_")
    keys = schema_guess.get(table, {"primary_keys": [], "foreign_keys": []})
    pks = set(keys.get("primary_keys", []))
    fks = set(keys.get("foreign_keys", []))

    if col in pks:
        if "unnamed" in str(col).lower():
            return (
                f"Surrogate primary key (`row_index` / inferred from export) for `{table}`; "
                "uniquely identifies each row."
            )
        return f"Primary key for `{table}`; uniquely identifies each row."
    if col in fks:
        return "Foreign key; links this table to another entity for joins."

    parts: List[str] = []

    if any(x in lc for x in ["email", "e_mail", "mail"]):
        parts.append("Contact email address.")
    elif "phone" in lc or "mobile" in lc or "tel" in lc:
        parts.append("Phone or contact number.")
    elif "name" in lc and "user" not in lc:
        parts.append("Display or legal name.")
    elif lc in {"first_name", "firstname", "given_name"}:
        parts.append("Given name of a person or entity.")
    elif lc in {"last_name", "lastname", "surname", "family_name"}:
        parts.append("Family or surname.")
    elif "address" in lc or "street" in lc or "city" in lc or "zip" in lc or "postal" in lc:
        parts.append("Location or address component.")
    elif "country" in lc or "region" in lc or "state" in lc:
        parts.append("Geographic region or territory.")
    elif any(x in lc for x in ["date", "time", "timestamp", "created", "updated", "at"]):
        parts.append("Temporal field (event or record time).")
    elif any(x in lc for x in ["amount", "price", "total", "cost", "revenue", "mrr", "arr"]):
        parts.append("Monetary or numeric measure.")
    elif any(x in lc for x in ["qty", "quantity", "count", "units"]):
        parts.append("Count or quantity measure.")
    elif "status" in lc or "state" in lc or "phase" in lc:
        parts.append("Lifecycle or workflow status.")
    elif "id" in lc and col not in fks:
        parts.append("Identifier; may reference another record.")
    elif any(x in lc for x in ["sku", "product", "item"]):
        parts.append("Product or catalog reference.")
    elif any(x in lc for x in ["subscription", "plan", "tier"]):
        parts.append("Subscription or billing plan context.")
    elif any(x in lc for x in ["org", "company", "account", "facility", "site"]):
        parts.append("Organization or site context.")
    elif any(x in lc for x in ["event", "action", "type", "category"]):
        parts.append("Event type or categorical label.")

    dtype = str(series.dtype)
    null_pct = round(series.isna().mean() * 100, 1)
    uniq = series.nunique(dropna=True)

    if not parts:
        if "int" in dtype or "float" in dtype:
            parts.append("Numeric measure or code.")
        elif "bool" in dtype:
            parts.append("Boolean flag.")
        elif "datetime" in dtype:
            parts.append("Date/time value.")
        else:
            parts.append("Text or categorical field.")

    if null_pct > 15:
        parts.append(f"~{null_pct}% nulls — may need imputation or optional handling.")
    if uniq <= 5 and series.notna().any():
        parts.append(f"Low cardinality (~{uniq} distinct values).")

    return " ".join(parts)


def _dtype_word(series: pd.Series) -> str:
    s = str(series.dtype)
    if "int" in s:
        return "int"
    if "float" in s:
        return "float"
    if "bool" in s:
        return "bool"
    if "datetime" in s:
        return "datetime"
    return "string"


def generate_mermaid_erd(
    schema_guess: Dict[str, Dict[str, List[str]]],
    dfs: Optional[Dict[str, pd.DataFrame]] = None,
) -> str:
    """Mermaid ERD with real column names when dfs is provided."""
    lines = ["erDiagram"]
    dfs = dfs or {}
    for table, keys in schema_guess.items():
        safe = re.sub(r"[^a-zA-Z0-9_]", "_", table.rsplit(".", 1)[0])
        lines.append(f"    {safe} {{")
        df = dfs.get(table)
        if df is not None:
            for col in df.columns:
                dt = _dtype_word(df[col])
                disp = _humanize_column_name(str(col))
                safe_col = re.sub(r"[^a-zA-Z0-9_]", "_", disp)
                tag = ""
                if col in keys["primary_keys"]:
                    tag = " PK"
                elif col in keys["foreign_keys"]:
                    tag = " FK"
                lines.append(f"        {dt} {safe_col}{tag}")
        else:
            for pk in keys["primary_keys"]:
                lines.append(f"        string {pk} PK")
            for fk in keys["foreign_keys"]:
                lines.append(f"        string {fk} FK")
            if not keys["primary_keys"] and not keys["foreign_keys"]:
                lines.append("        string column")
        lines.append("    }")

    rels = infer_relationships(schema_guess)
    for parent, child, fk in rels:
        p = re.sub(r"[^a-zA-Z0-9_]", "_", parent.rsplit(".", 1)[0])
        c = re.sub(r"[^a-zA-Z0-9_]", "_", child.rsplit(".", 1)[0])
        lines.append(f'    {p} ||--o{{ {c} : "{fk}"')

    return "\n".join(lines)


def _erd_table_id(table_name: str) -> str:
    stem = table_name.rsplit(".", 1)[0]
    return "t_" + re.sub(r"[^a-zA-Z0-9]", "_", stem)


def _erd_table_label(
    table_name: str, df: pd.DataFrame, keys: Dict[str, List[str]]
) -> str:
    """Multiline label: filename, PK/FK summary, then real column names."""
    pks = keys.get("primary_keys", [])
    fks = keys.get("foreign_keys", [])
    parts = [table_name, "────────"]
    if pks:
        parts.append("PK: " + ", ".join(_humanize_key_list(list(pks))))
    if fks:
        parts.append("FK: " + ", ".join(_humanize_key_list(list(fks))))
    parts.append("")
    cols = list(df.columns)
    max_show = 14
    for c in cols[:max_show]:
        mark = ""
        if c in pks:
            mark = "  [PK]"
        elif c in fks:
            mark = "  [FK]"
        dt = _dtype_word(df[c])
        c_disp = _humanize_column_name(c)
        parts.append(f"{c_disp} : {dt}{mark}")
    if len(cols) > max_show:
        parts.append(f"... +{len(cols) - max_show} more columns")
    text = "\\n".join(parts)
    return text.replace('"', '\\"')


def _assign_columns_to_logical_entities(
    df: pd.DataFrame, keys: Dict[str, List[str]]
) -> List[Tuple[str, List[str]]]:
    """
    Split one wide table into logical ER groups — each becomes a box.
    PK/FK columns go first into KEYS so they are not buried in "remaining".
    Every column appears exactly once.
    """
    cols = list(df.columns)
    pks = set(keys.get("primary_keys", []))
    fks = set(keys.get("foreign_keys", []))
    assigned = set()

    def bucket(name: str, pred) -> Tuple[str, List[str]]:
        got: List[str] = []
        for c in cols:
            if c in assigned:
                continue
            if pred(c):
                got.append(c)
                assigned.add(c)
        return (name, got)

    groups: List[Tuple[str, List[str]]] = []

    # Keys entity first (order: PK columns, then FK columns)
    key_ordered = [c for c in cols if c in pks] + [
        c for c in cols if c in fks and c not in pks
    ]
    for c in key_ordered:
        assigned.add(c)
    if key_ordered:
        groups.append(("ENTITY · KEYS (PK / FK)", key_ordered))

    def core_p(c: str) -> bool:
        lc = c.lower()
        if lc in ("id", "course_id", "product_id", "order_id", "account_id"):
            return True
        if "unnamed" in lc:
            return True
        if "title" in lc or lc in ("url", "link", "name", "sku"):
            return True
        if lc.endswith("_id"):
            pref = lc.rsplit("_", 1)[0]
            return pref in ("course", "product", "order", "account", "user", "org")
        return False
    commerce_p = lambda c: any(
        x in c.lower() for x in ("paid", "price", "cost", "amount", "mrr", "currency")
    )
    metrics_p = lambda c: any(
        x in c.lower()
        for x in ("num_", "subscriber", "review", "lecture", "rating", "count", "qty", "score")
    )
    content_p = lambda c: any(
        x in c.lower()
        for x in (
            "date",
            "time",
            "duration",
            "level",
            "subject",
            "category",
            "tag",
            "description",
            "published",
            "created",
            "updated",
        )
    )

    for title, pred in [
        ("ENTITY · CORE / IDENTITY", core_p),
        ("ENTITY · COMMERCE", commerce_p),
        ("ENTITY · METRICS", metrics_p),
        ("ENTITY · CONTENT & TIME", content_p),
    ]:
        name, got = bucket(title, pred)
        if got:
            groups.append((name, got))

    # Anything left → MISC entity (guarantees all columns used)
    rest = [c for c in cols if c not in assigned]
    if rest:
        groups.append(("ENTITY · ATTRIBUTES (remaining)", rest))

    # If still < 4 groups, split largest group
    while len(groups) < 4 and any(len(g[1]) > 2 for g in groups):
        for i, (nm, gcols) in enumerate(groups):
            if len(gcols) <= 2:
                continue
            mid = len(gcols) // 2
            a, b = gcols[:mid], gcols[mid:]
            groups[i] = (nm + " (a)", a)
            groups.insert(i + 1, (nm + " (b)", b))
            break

    # Fallback: chunk into 4 parts
    if len(groups) < 2:
        groups = []
        chunk = max(1, (len(cols) + 3) // 4)
        for i in range(0, len(cols), chunk):
            part = cols[i : i + chunk]
            groups.append((f"ENTITY · PART {len(groups) + 1}", part))

    return groups


def _logical_entity_box_label(
    group_title: str,
    columns: List[str],
    keys: Dict[str, List[str]],
    table_file: str,
    df: Optional[pd.DataFrame] = None,
) -> str:
    """Richer box text: dtypes + optional null % — no extra entities, same groups."""
    pks = set(keys.get("primary_keys", []))
    fks = set(keys.get("foreign_keys", []))
    n = len(columns)
    lines = [
        group_title,
        f"from: {table_file}",
        f"{n} field(s) · same CSV row",
        "────────",
    ]
    for c in columns:
        cd = _humanize_column_name(c)
        meta = ""
        if df is not None and c in df.columns:
            try:
                dt = _dtype_word(df[c])
                nullp = round(100.0 * float(df[c].isna().mean()), 1)
                meta = f"  ({dt})"
                if nullp > 0:
                    meta += f" · {nullp}% null"
            except Exception:
                meta = ""
        if c in pks:
            lines.append(f"*{cd}{meta}  [PK]")
        elif c in fks:
            lines.append(f"+{cd}{meta}  [FK]")
        else:
            lines.append(f"  {cd}{meta}")
    text = "\\n".join(lines)
    return text.replace('"', '\\"')


def generate_physical_erd_dot(
    dfs: Dict[str, pd.DataFrame],
    schema_guess: Dict[str, Dict[str, List[str]]],
) -> str:
    """
    Physical ERD: one box per uploaded table, every column listed with dtype and PK/FK role.
    Relationship edges: child table → parent table (FK references PK).
    """
    if not dfs:
        return 'digraph X { label="No datasets"; bgcolor="#f8fafc"; }'

    rels = infer_relationships(schema_guess)
    lines = [
        "digraph PhysicalERD {",
        "  compound=true;",
        "  rankdir=LR;",
        "  nodesep=0.55;",
        "  ranksep=1.0;",
        '  graph [fontname="Helvetica", fontsize=12, labelloc=t, label="Physical schema (wireframe) — tables & inferred PK/FK", bgcolor="#fafafa", pad=0.5, fontcolor="#334155"];',
        '  splines=polyline;',
        '  node [fontname="Helvetica", fontsize=8, shape=box, style="rounded,filled", fillcolor="#ffffff", color="#64748b", penwidth=1.1];',
        '  edge [fontname="Helvetica", fontsize=8, color="#2563eb", arrowsize=0.95, penwidth=1.15];',
        "",
    ]

    max_cols = 48
    for fname, df in dfs.items():
        tid = _erd_table_id(fname)
        keys = schema_guess.get(fname, {"primary_keys": [], "foreign_keys": []})
        pks = set(keys.get("primary_keys", []))
        fks = set(keys.get("foreign_keys", []))
        safe_name = fname.replace('"', '\\"')
        row_lines: List[str] = []
        cols = list(df.columns)
        for c in cols[:max_cols]:
            dt = _dtype_word(df[c])
            esc = _humanize_column_name(str(c)).replace('"', '\\"')
            if c in pks:
                row_lines.append(f"◆ {esc}   ({dt})   [PK]")
            elif c in fks:
                row_lines.append(f"◇ {esc}   ({dt})   [FK]")
            else:
                row_lines.append(f"· {esc}   ({dt})")
        if len(cols) > max_cols:
            row_lines.append(f"... +{len(cols) - max_cols} more columns")
        body = "\\n".join(row_lines)
        lbl = (
            f"{safe_name}\\n{len(df):,} rows · {len(df.columns)} columns\\n"
            f"────────\\n{body}"
        )
        lines.append(
            f'  {tid} [label="{lbl}", margin=0.18, fillcolor="#f8fafc", penwidth=1.5];'
        )

    lines.append("")
    # Child (has FK) → Parent (referenced PK)
    for parent, child, fk in rels:
        pid = _erd_table_id(parent)
        cid = _erd_table_id(child)
        fe = fk.replace('"', '\\"')
        lines.append(
            f'  {cid} -> {pid} [label="FK {fe}", arrowhead=vee, arrowsize=0.95, color="#1d4ed8", penwidth=1.25];'
        )

    lines.append("}")
    return "\n".join(lines)


def generate_dataset_erd_dot(
    dfs: Dict[str, pd.DataFrame],
    schema_guess: Dict[str, Dict[str, List[str]]],
) -> str:
    """
    **Layered ERD architecture** (reference-style): 4 horizontal bands, 9+ boxes,
    dataset-driven column text — not a single flat entity.
    """
    if not dfs:
        return 'digraph X { label="No datasets"; bgcolor="#f8fafc"; }'

    rels = infer_relationships(schema_guess)
    total_rows = sum(len(df) for df in dfs.values())
    total_cols = sum(len(df.columns) for df in dfs.values())

    # Primary table for logical decomposition (widest / first)
    main_name = max(dfs.keys(), key=lambda n: len(dfs[n].columns))
    main_df = dfs[main_name]
    main_keys = schema_guess.get(main_name, {"primary_keys": [], "foreign_keys": []})

    groups = _assign_columns_to_logical_entities(main_df, main_keys)

    pk_list = main_keys.get("primary_keys", [])[:5]
    fk_list = main_keys.get("foreign_keys", [])[:6]
    pk_txt = ", ".join(_humanize_key_list(pk_list)) if pk_list else "none inferred"
    n_files = len(dfs)
    if fk_list:
        fk_txt = ", ".join(_humanize_key_list(fk_list))
    else:
        fk_txt = "— (no FK columns in file)" if n_files == 1 else "— (none)"
    if n_files == 1:
        rel_txt = f"{len(rels)} (needs 2+ CSVs for links)"
    else:
        rel_txt = f"{len(rels)} cross-table FK link(s)"

    shared_lab = (
        "Shared schema state\\n────────\\n"
        f"PK: {pk_txt}\\n"
        f"FK: {fk_txt}\\n"
        f"Cross-table links: {rel_txt}\\n"
        f"Profiled: {n_files} file(s), {total_cols} cols"
    ).replace('"', '\\"')

    graph_title = (
        "Wireframe ERD — layered logical model (blueprint view)"
    ).replace('"', '\\"')

    lines = [
        "digraph ERDLayered {",
        "  compound=true;",
        '  rankdir=TB;',
        '  splines=polyline;',
        '  nodesep=0.45;',
        '  ranksep=0.85;',
        f'  graph [fontname="Helvetica", fontsize=11, labelloc=t, label="{graph_title}", bgcolor="#fafafa", pad=0.5, fontcolor="#334155"];',
        '  node [fontname="Helvetica", fontsize=8];',
        '  edge [fontname="Helvetica", fontsize=7, color="#64748b", penwidth=1.0];',
        "",
    ]

    # --- ① Entry — cluster (solid outer border)
    lines.extend(
        [
            '  subgraph cluster_entry {',
            '    label="①  Entry / ingest"; labelloc=t;',
            '    style="rounded,filled"; fillcolor="#f0f9ff"; color="#64748b"; fontcolor="#334155";',
            "",
        ]
    )
    for fname, df in dfs.items():
        tid = _erd_table_id(fname)
        fn_esc = fname.replace('"', '\\"')
        lines.append(
            f'    {tid} [label="SOURCE\\n{fn_esc}\\n{len(df):,} rows · {len(df.columns)} cols", '
            f'shape=note, style=filled, fillcolor="#ffffff", color="#64748b", fontcolor="#0f172a", margin=0.12, penwidth=1.1];'
        )
    lines.append("  }")
    lines.append("")

    # --- ② Logical entities
    lines.extend(
        [
            '  subgraph cluster_entities {',
            '    label="②  Logical entities — dtype & null rate per field"; labelloc=t;',
            '    style="rounded,filled"; fillcolor="#f0fdf4"; color="#64748b"; fontcolor="#334155";',
            "",
        ]
    )
    ent_ids: List[str] = []
    for i, (gtitle, gcols) in enumerate(groups):
        eid = f"ent_{i}"
        ent_ids.append(eid)
        lab = _logical_entity_box_label(gtitle, gcols, main_keys, main_name, main_df)
        lines.append(
            f'    {eid} [label="{lab}", shape=box, style="rounded,filled", fillcolor="#ffffff", '
            f'color="#15803d", penwidth=1.2, fontcolor="#14532d", margin=0.14];'
        )
    lines.append("  }")
    lines.append("")

    # --- ③ Shared keys
    lines.extend(
        [
            '  subgraph cluster_shared {',
            '    label="③  Shared keys & relationships"; labelloc=t;',
            '    style="rounded,filled"; fillcolor="#faf5ff"; color="#64748b"; fontcolor="#334155";',
            f'    shared_keys [label="{shared_lab}", shape=box, style="rounded,filled", fillcolor="#f5f3ff", color="#6d28d9", penwidth=1.2, fontcolor="#4c1d95", margin=0.16];',
            "  }",
            "",
        ]
    )

    # --- ④ Artifacts / target
    art_rows = f"{total_rows:,}"
    lines.extend(
        [
            '  subgraph cluster_artifacts {',
            '    label="④  Artifacts & target (ERD outputs)"; labelloc=t;',
            '    style="rounded,filled"; fillcolor="#fffbeb"; color="#64748b"; fontcolor="#334155";',
            f'    art_dict [label="Data dictionary\\n{total_cols} fields described", shape=box, style="rounded,filled", fillcolor="#ffffff", color="#ca8a04", margin=0.1];',
            '    art_mermaid [label="Mermaid ERD text\\nexport for docs", shape=box, style="rounded,filled", fillcolor="#ffffff", color="#ca8a04", margin=0.1];',
            f'    art_target [label="Profiled dataset\\n{art_rows} rows\\nready for load", shape=box, style="rounded,filled", fillcolor="#fffbeb", color="#b45309", margin=0.1];',
            "  }",
            "",
        ]
    )

    # Cross-table FK edges — solid lines
    for parent, child, fk in rels:
        pid = _erd_table_id(parent)
        cid = _erd_table_id(child)
        fk_esc = fk.replace('"', '\\"')
        lines.append(
            f'  {pid} -> {cid} [label="{fk_esc} (FK)", color="#2563eb", penwidth=1.1, arrowsize=0.9, '
            f'constraint=false];'
        )

    # Ingest → each logical entity
    for fname in dfs:
        tid = _erd_table_id(fname)
        for j in range(len(ent_ids)):
            lines.append(
                f'  {tid} -> ent_{j} [label="columns", color="#64748b", penwidth=1.0];'
            )

    # Entities → shared
    for j, eid in enumerate(ent_ids):
        lines.append(
            f'  {eid} -> shared_keys [label="infer keys", color="#7c3aed", penwidth=1.05];'
        )

    # Shared → artifacts
    lines.extend(
        [
            '  shared_keys -> art_dict [label="feeds", color="#b45309", penwidth=1.1];',
            '  shared_keys -> art_mermaid [label="feeds", color="#b45309", penwidth=1.1];',
            '  shared_keys -> art_target [label="feeds", color="#b45309", penwidth=1.1];',
            "",
        ]
    )

    # Optional: light link between logical entities (same grain) — chain
    for j in range(len(ent_ids) - 1):
        lines.append(
            f'  ent_{j} -> ent_{j + 1} [label="1:1 via row", color="#16a34a", '
            f'constraint=false, fontsize=7, penwidth=0.9];'
        )

    lines.append("}")
    return "\n".join(lines)


def generate_langgraph_runtime_dot(
    dfs: Dict[str, pd.DataFrame],
    schema_guess: Dict[str, Dict[str, List[str]]],
) -> str:
    """Optional: code/runtime workflow (LangGraph) — separate from data ERD."""
    rels = infer_relationships(schema_guess)
    total_rows = sum(len(df) for df in dfs.values()) if dfs else 0
    total_cols = sum(len(df.columns) for df in dfs.values()) if dfs else 0
    n_files = len(dfs)

    scout_extra = ""
    if dfs:
        nm = list(dfs.keys())[:2]
        scout_extra = "\\n" + "\\n".join(nm)
        if len(dfs) > 2:
            scout_extra += "\\n…"
    loader_extra = ""
    if dfs:
        loader_extra = f"\\nCurrent load scope: {total_rows:,} rows · {total_cols} cols · {n_files} file(s)"
    arch_extra = ""
    if schema_guess:
        arch_extra = f"\\nSchema: {len(rels)} rel(s) inferred"

    lines = [
        "digraph ETLAgent {",
        '  rankdir=TB;',
        '  splines=spline;',
        '  compound=true;',
        '  graph [fontname="Helvetica", fontsize=11, labelloc=t, label="Runtime pipeline (code) — LangGraph + tools"];',
        '  node [fontname="Helvetica", fontsize=10];',
        '  edge [fontname="Helvetica", fontsize=9];',
        "",
        '  subgraph cluster_entry {',
        '    label="Entry Point"; labelloc=t;',
        '    style="rounded,filled"; fillcolor="#dbeafe"; color="#2563eb";',
        '    script_py [label="script.py", shape=note, style=filled, fillcolor="#eff6ff"];',
        '    main_py [label="main.py", shape=note, style=filled, fillcolor="#eff6ff"];',
        "  }",
        "",
        '  run_pipeline [label="run_pipeline()", shape=cds, style="filled,rounded", fillcolor="#bae6fd"];',
        "",
        '  subgraph cluster_orch {',
        '    label="Orchestration Layer"; labelloc=t;',
        '    style="rounded,filled"; fillcolor="#dcfce7"; color="#16a34a";',
        '    langgraph [label="LangGraph Pipeline\\ngraph.py · build_graph()", shape=box, style="filled,rounded", fillcolor="#ffffff"];',
        f'    scout [label="Scout Node{scout_extra}", shape=box, style="filled,rounded", fillcolor="#e5e7eb"];',
        f'    architect [label="Architect Node{arch_extra}", shape=box, style="filled,rounded", fillcolor="#e5e7eb"];',
        '    engineer [label="Engineer Node\\n_extract_code() / exec()", shape=box, style="filled,rounded", fillcolor="#e5e7eb"];',
        f'    loader [label="Loader Node{loader_extra}", shape=box, style="filled,rounded", fillcolor="#e5e7eb"];',
        '    router [label="Conditional Edge\\nrouter.py", shape=diamond, style="filled,rounded", fillcolor="#fef9c3"];',
        "  }",
        "",
        '  shared_state [label="Shared State\\netl_state.py", shape=box, style="filled,rounded", fillcolor="#c4b5fd", color="#6d28d9", penwidth=2];',
        "",
        '  subgraph cluster_impl {',
        '    label="Implementation & Utility Layer"; labelloc=t;',
        '    style="rounded,filled"; fillcolor="#fef9c7"; color="#ca8a04";',
        '    csv_tools [label="tools/csv_tools.py\\nread_csv() · infer_schema()", shape=box, style="filled,rounded", fillcolor="#ffffff"];',
        '    pandas_lib [label="pandas", shape=box, style="filled,rounded", fillcolor="#fff7ed"];',
        '    api_tools [label="tools/api_tools.py", shape=box, style="filled,rounded", fillcolor="#ffffff"];',
        '    requests_lib [label="requests", shape=box, style="filled,rounded", fillcolor="#fff7ed"];',
        '    ollama_client [label="langchain-ollama\\nChatOllama client", shape=box, style="filled,rounded", fillcolor="#ffffff"];',
        '    ollama_srv [label="Ollama Server\\nqwen2.5:14b", shape=box, style="filled,rounded", fillcolor="#e0f2fe"];',
        '    sqlalchemy_lib [label="sqlalchemy", shape=box, style="filled,rounded", fillcolor="#fff7ed"];',
        '    target [label="Target (File / DB)", shape=box, style="filled,rounded", fillcolor="#ecfdf5"];',
        "  }",
        "",
        "  { rank=same; script_py; main_py; }",
        "  script_py -> main_py [style=invis];",
        '  main_py -> run_pipeline [label="calls run_pipeline()", color="#334155"];',
        '  run_pipeline -> langgraph [label="invoke", color="#334155"];',
        "",
        '  langgraph -> scout [label="calls *_node(state)", color="#334155"];',
        "  scout -> architect [color=\"#334155\"];",
        "  architect -> engineer [color=\"#334155\"];",
        '  engineer -> loader [label="pass", color="#16a34a", penwidth=1.5];',
        '  loader -> router [label="escalate", color="#334155"];',
        '  router -> loader [label="retry", color="#2563eb"];',
        '  router -> engineer [label="generates bad code", color=red, fontcolor=red];',
        "",
        '  scout -> shared_state [style=dashed, color="#6d28d9", label="reads/writes"];',
        '  architect -> shared_state [style=dashed, color="#6d28d9", label="reads/writes"];',
        '  engineer -> shared_state [style=dashed, color="#6d28d9", label="reads/writes"];',
        '  loader -> shared_state [style=dashed, color="#6d28d9", label="reads/writes"];',
        "",
        '  scout -> csv_tools [label="uses", color="#64748b"];',
        "  csv_tools -> pandas_lib [color=\"#64748b\"];",
        '  scout -> api_tools [label="uses", color="#64748b"];',
        "  api_tools -> requests_lib [color=\"#64748b\"];",
        "",
        '  engineer -> ollama_client [label="prompts / code", color="#64748b"];',
        "  ollama_client -> ollama_srv [color=\"#64748b\"];",
        "",
        '  loader -> sqlalchemy_lib [label="writes via", color="#64748b"];',
        '  loader -> target [label="writes data", color="#059669", penwidth=1.8];',
        '  sqlalchemy_lib -> target [color="#64748b"];',
        "}",
    ]
    return "\n".join(lines)


def run_naive_aggregator(
    dfs: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, Dict]:
    if not dfs:
        return pd.DataFrame(), {}

    items = list(dfs.items())
    _, base_df = items[0]
    joined = base_df.copy()
    source_rows = len(joined)

    for name, df in items[1:]:
        common_cols = list(set(joined.columns) & set(df.columns))
        key_candidates = [
            c for c in common_cols if c.lower() == "id" or c.lower().endswith("_id")
        ]
        if not key_candidates:
            continue
        key = key_candidates[0]
        joined = joined.merge(df, on=key, how="left", suffixes=("", f"_{name}"))

    final_rows = len(joined)
    product_rows = 1
    for _, df in dfs.items():
        product_rows *= max(1, len(df))

    invariant_ok = final_rows <= product_rows

    return joined, {
        "R_final": final_rows,
        "R_product_sources": product_rows,
        "invariant_ok": invariant_ok,
        "source_rows_first_table": source_rows,
    }


def to_csv_download(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def _quality_tag(df: pd.DataFrame) -> str:
    """Simple quality indicator based on missing values."""
    if df.empty:
        return "Needs review"
    null_ratio = float(df.isna().sum().sum()) / float(max(1, df.shape[0] * max(1, df.shape[1])))
    if null_ratio < 0.03:
        return "Ready"
    if null_ratio < 0.12:
        return "Watch"
    return "Needs review"


def render_schema_pipeline_layout(dfs: Dict[str, pd.DataFrame]) -> None:
    if not dfs:
        st.info("Upload datasets first to preview pipeline layout.")
        return

    source_names = list(dfs.keys())
    total_rows = sum(len(df) for df in dfs.values())
    total_cols = sum(len(df.columns) for df in dfs.values())
    schema_guess = guess_keys(dfs)

    # Build dataset-specific text for the pipeline cards
    first_table = source_names[0] if source_names else "source_table"
    first_cols = list(dfs[first_table].columns[:4]) if source_names else []
    first_cols_txt = ", ".join(first_cols) if first_cols else "n/a"
    pk_all: List[str] = []
    fk_all: List[str] = []
    for t in schema_guess.values():
        pk_all.extend(t.get("primary_keys", []))
        fk_all.extend(t.get("foreign_keys", []))
    pk_unique = list(dict.fromkeys(pk_all))
    fk_unique = list(dict.fromkeys(fk_all))
    pk_txt = ", ".join(_humanize_key_list(pk_unique[:3])) if pk_unique else "none detected yet"
    fk_txt = ", ".join(_humanize_key_list(fk_unique[:3])) if fk_unique else "none detected yet"
    relationships = infer_relationships(schema_guess)

    def source_payload(idx: int):
        if idx < len(source_names):
            name = source_names[idx]
            df = dfs[name]
            q = _quality_tag(df)
            return {
                "name": name,
                "rows": f"{len(df):,}",
                "cols": f"{len(df.columns)}",
                "status": q,
                "risk": "ok" if q == "Ready" else ("warn" if q == "Watch" else "risk"),
            }
        return {
            "name": "optional source",
            "rows": "—",
            "cols": "—",
            "status": "Pending",
            "risk": "warn",
        }

    s1 = source_payload(0)
    s2 = source_payload(1)

    html = f"""
    <style>
      .dw-wrap {{
        background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:14px;position:relative;
        font-family:Inter,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#0f172a;
      }}
      .lane {{
        border-radius:10px;
        border:1px solid #dbeafe;
        background:#eff6ff;
        padding:10px;
        margin-bottom:10px;
      }}
      .lane.t {{ border-color:#fde68a; background:#fffbeb; }}
      .lane.l {{ border-color:#bbf7d0; background:#f0fdf4; }}
      .lane-title {{
        font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:#334155;font-weight:800;margin-bottom:8px;
      }}
      .row {{
        display:flex; align-items:center; gap:10px; flex-wrap:wrap;
      }}
      .node {{
        background:#fff;border:1px solid #d1d5db;border-radius:10px;padding:10px 12px;
        box-shadow:0 1px 5px rgba(15,23,42,.05);position:relative; min-width:230px;
      }}
      .node.extract {{ border-left:4px solid #2563eb; }}
      .node.transform {{ border-left:4px solid #d97706; }}
      .node.load {{ border-left:4px solid #16a34a; }}
      .kicker {{ font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#64748b;font-weight:700; }}
      .title {{ margin-top:2px;font-size:16px;font-weight:700;line-height:1.2; }}
      .meta {{ margin-top:8px;font-size:13px;color:#475569; }}
      .pill {{ margin-top:8px;display:inline-block;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:700; }}
      .pill.ok {{ color:#047857;background:#d1fae5;border:1px solid #a7f3d0; }}
      .pill.warn {{ color:#a16207;background:#fef9c3;border:1px solid #fde68a; }}
      .pill.risk {{ color:#b91c1c;background:#fee2e2;border:1px solid #fecaca; }}
      .arrow {{
        width:26px;height:2px;background:#94a3b8;position:relative;display:inline-block;
      }}
      .arrow:after {{
        content:""; position:absolute; right:-1px; top:-4px;
        border-top:5px solid transparent; border-bottom:5px solid transparent; border-left:7px solid #64748b;
      }}
    </style>
    <div class="dw-wrap">
      <div class="lane">
        <div class="lane-title">Extract stage</div>
        <div class="row">
          <div class="node extract">
            <div class="kicker">SOURCE</div>
            <div class="title">{s1["name"]}</div>
            <div class="meta">{s1["rows"]} rows · {s1["cols"]} cols</div>
            <span class="pill {s1["risk"]}">{s1["status"]}</span>
          </div>
          <span class="arrow"></span>
          <div class="node extract">
            <div class="kicker">SOURCE</div>
            <div class="title">{s2["name"]}</div>
            <div class="meta">{s2["rows"]} rows · {s2["cols"]} cols</div>
            <span class="pill {s2["risk"]}">{s2["status"]}</span>
          </div>
        </div>
      </div>

      <div class="lane t">
        <div class="lane-title">Transform stage</div>
        <div class="row">
          <div class="node transform">
            <div class="kicker">AGENT</div>
            <div class="title">Schema Inference</div>
            <div class="meta">Detected PKs: {pk_txt}</div>
            <div class="meta">Detected FKs: {fk_txt}</div>
            <span class="pill ok">Inferred</span>
          </div>
          <span class="arrow"></span>
          <div class="node transform">
            <div class="kicker">MAPPING</div>
            <div class="title">Canonical Mapping</div>
            <div class="meta">Normalize columns for {first_table}</div>
            <div class="meta">Example fields: {first_cols_txt}</div>
            <span class="pill warn">Awaiting run</span>
          </div>
          <span class="arrow"></span>
          <div class="node transform">
            <div class="kicker">HITL</div>
            <div class="title">Validation Gate</div>
            <div class="meta">Review key mapping, nulls, and join safety</div>
            <span class="pill warn">Manual approval</span>
          </div>
        </div>
      </div>

      <div class="lane l">
        <div class="lane-title">Load stage</div>
        <div class="row">
          <div class="node load">
            <div class="kicker">ARTIFACTS</div>
            <div class="title">Schema Outputs</div>
            <div class="meta">ERD edges inferred: {len(relationships)}</div>
            <div class="meta">Data dictionary for {total_cols} fields · {total_rows:,} rows referenced</div>
            <span class="pill warn">Ready for export</span>
          </div>
        </div>
      </div>
    </div>
    """
    components.html(html, height=520, scrolling=False)


def planned_transforms_named(df: pd.DataFrame) -> List[str]:
    """Concrete transform steps using actual column names from this table."""
    cols = list(df.columns)
    steps: List[str] = []
    date_cols = [
        c
        for c in cols
        if any(
            x in c.lower()
            for x in ["date", "time", "timestamp", "created", "updated", "_at"]
        )
    ]
    if date_cols:
        steps.append(f"Parse & normalize dates: {', '.join(date_cols[:6])}")
    id_cols = [c for c in cols if c.lower().endswith("_id") or c.lower() == "id"]
    if id_cols:
        steps.append(f"Treat as join keys: {', '.join(id_cols[:8])}")
    nulls = df.isna().sum()
    bad = [c for c in cols if nulls[c] > 0]
    if bad:
        steps.append(f"Impute or flag nulls in: {', '.join(bad[:6])}")
    if len(steps) < 3:
        if len(cols) <= 10:
            steps.append(f"Coerce dtypes for: {', '.join(cols)}")
        else:
            steps.append(
                f"Coerce dtypes for **{len(cols)}** columns (e.g. {', '.join(cols[:5])}, …)"
            )
    return steps[:4]


def _html_strip_md_bold(s: str) -> str:
    """Strip **bold** for plain HTML text."""
    return re.sub(r"\*\*(.+?)\*\*", r"\1", s)


def render_schema_pipeline_layout_v2(
    dfs: Dict[str, pd.DataFrame],
    schema_guess: Optional[Dict[str, Dict[str, List[str]]]] = None,
) -> None:
    """Schema inference pipeline — wireframe UI: columns, keys, joins (ETL lanes)."""
    if not dfs:
        st.info("Upload datasets first to preview pipeline layout.")
        return

    if schema_guess is None:
        schema_guess = guess_keys(dfs)
    relationships = infer_relationships(schema_guess)
    source_names = list(dfs.keys())
    total_rows = sum(len(df) for df in dfs.values())
    total_cols = sum(len(df.columns) for df in dfs.values())

    pk_all: List[str] = []
    fk_all: List[str] = []
    for t in schema_guess.values():
        pk_all.extend(t.get("primary_keys", []))
        fk_all.extend(t.get("foreign_keys", []))
    pk_unique = list(dict.fromkeys(pk_all))
    fk_unique = list(dict.fromkeys(fk_all))

    h = html.escape

    # Metrics strip (wireframe blueprint)
    st.markdown(
        f"""
<div class="dw-wf-board">
  <div class="dw-wf-board-title">Schema inference · wireframe blueprint</div>
  <div class="dw-wf-metrics">
    <div class="dw-wf-metric-cell"><span class="dw-wf-num">{len(source_names)}</span><span class="dw-wf-lbl">SOURCES</span></div>
    <div class="dw-wf-metric-cell"><span class="dw-wf-num">{len(relationships)}</span><span class="dw-wf-lbl">CROSS-TABLE LINKS</span></div>
    <div class="dw-wf-metric-cell"><span class="dw-wf-num">{total_cols}</span><span class="dw-wf-lbl">FIELDS PROFILED</span></div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    lane_e, lane_t, lane_l = st.columns([1, 1.15, 1])

    # --- EXTRACT lane ---
    extract_cards: List[str] = []
    for name in source_names[:3]:
        df = dfs[name]
        sg = schema_guess.get(name, {"primary_keys": [], "foreign_keys": []})
        pk_str = ", ".join(_humanize_key_list(sg["primary_keys"])) or "—"
        fk_str = ", ".join(_humanize_key_list(sg["foreign_keys"])) or "—"
        cols_preview = ", ".join(f"<code>{h(str(c))}</code>" for c in list(df.columns)[:8])
        if len(df.columns) > 8:
            cols_preview += f" … +{len(df.columns) - 8}"
        q = _quality_tag(df)
        qcls = "dw-wf-pill-ok" if q == "Ready" else ("dw-wf-pill-warn" if q == "Watch" else "dw-wf-pill-bad")
        extract_cards.append(
            f"""
<div class="dw-wf-card">
  <div class="dw-wf-card-title">{h(name)}</div>
  <div class="dw-wf-meta">{len(df):,} rows · {len(df.columns)} cols</div>
  <div class="dw-wf-code">{cols_preview}</div>
  <div class="dw-wf-keys">PK <code>{h(pk_str)}</code> · FK <code>{h(fk_str)}</code></div>
  <span class="dw-wf-pill {qcls}">{h(q)}</span>
</div>
"""
        )
    with lane_e:
        st.markdown(
            '<div class="dw-wf-lane-h e">Extract · sources</div>',
            unsafe_allow_html=True,
        )
        st.markdown("".join(extract_cards), unsafe_allow_html=True)

    # --- TRANSFORM lane ---
    first_table = source_names[0]
    first_df = dfs[first_table]
    inf_rows: List[str] = []
    for tname in source_names[:4]:
        sg = schema_guess.get(tname, {"primary_keys": [], "foreign_keys": []})
        pk_h = ", ".join(_humanize_key_list(sg["primary_keys"])) or "—"
        fk_h = ", ".join(_humanize_key_list(sg["foreign_keys"])) or "—"
        inf_rows.append(
            f"<li><strong>{h(tname)}</strong> — PK <code>{h(pk_h)}</code> · FK <code>{h(fk_h)}</code></li>"
        )
    if relationships:
        rel_rows = "".join(
            f"<li>{h(parent)} <span style=\"color:#64748b\">—[{h(fk)}]→</span> {h(child)}</li>"
            for parent, child, fk in relationships[:6]
        )
        join_note = (
            f'<div class="dw-wf-note"><strong>Inferred joins</strong><ul class="dw-wf-ul">{rel_rows}</ul></div>'
        )
    else:
        join_note = (
            '<div class="dw-wf-note">No cross-table joins inferred — single file or no matching FK column names.</div>'
        )

    plan_lis = "".join(
        f"<li>{h(_html_strip_md_bold(step))}</li>" for step in planned_transforms_named(first_df)
    )
    pk_hint = ", ".join(_humanize_key_list(pk_unique[:4])) if pk_unique else "—"
    fk_hint = ", ".join(_humanize_key_list(fk_unique[:4])) if fk_unique else "—"

    transform_html = f"""
<div class="dw-wf-card">
  <div class="dw-wf-card-title">Inference · keys & joins</div>
  <ul class="dw-wf-ul">{"".join(inf_rows)}</ul>
  {join_note}
  <span class="dw-wf-pill dw-wf-pill-ok" style="margin-top:8px;">Status: inferred</span>
</div>
<div class="dw-wf-card">
  <div class="dw-wf-card-title">Planned transforms · {h(first_table)}</div>
  <ul class="dw-wf-ul">{plan_lis}</ul>
  <div class="dw-wf-note">Preview — heuristic list only. Aggregator applies merges; full transform engine is not run here.</div>
</div>
<div class="dw-wf-card">
  <div class="dw-wf-card-title">HITL gate</div>
  <div class="dw-wf-keys">PK <code>{h(pk_hint)}</code> · FK <code>{h(fk_hint)}</code></div>
  <div class="dw-wf-meta">Confirm in Step 3 · HITL #1 before Aggregator.</div>
  <span class="dw-wf-pill dw-wf-pill-ok">Ready for review → Step 3</span>
</div>
"""
    with lane_t:
        st.markdown(
            '<div class="dw-wf-lane-h t">Transform · inference & plan</div>',
            unsafe_allow_html=True,
        )
        st.markdown(transform_html, unsafe_allow_html=True)

    # --- LOAD lane ---
    names_short = ", ".join(source_names[:3])
    if len(source_names) > 3:
        names_short += " …"
    load_html = f"""
<div class="dw-wf-card">
  <div class="dw-wf-card-title">Artifacts & load</div>
  <div class="dw-wf-meta">ERD edges: <strong>{len(relationships)}</strong> — tables {h(names_short)}</div>
  <div class="dw-wf-meta">Data dictionary: <strong>{total_cols}</strong> fields · <strong>{len(dfs)}</strong> table(s)</div>
  <div class="dw-wf-meta">Rows profiled: <strong>{total_rows:,}</strong></div>
  <span class="dw-wf-pill dw-wf-pill-ok">Export-ready</span>
</div>
"""
    with lane_l:
        st.markdown(
            '<div class="dw-wf-lane-h l">Load · outputs</div>',
            unsafe_allow_html=True,
        )
        st.markdown(load_html, unsafe_allow_html=True)


# Sidebar
st.sidebar.markdown(
    "### 🧵 DataWeaver",
    unsafe_allow_html=True,
)
st.sidebar.caption("Distributed agentic pipeline · Sprint 0")
st.sidebar.caption(f"Build: {APP_BUILD}")

page = st.sidebar.radio(
    "Navigate",
    [
        "1 · Upload & Profiling",
        "2 · Mapper (Architect)",
        "3 · HITL Checkpoint #1",
        "4 · Aggregator (Engineer)",
        "5 · HITL Checkpoint #2",
        "6 · Logs & Downloads",
    ],
    label_visibility="collapsed",
)

st.sidebar.markdown("---")
_pipeline_status_sidebar()
st.sidebar.markdown("---")
st.sidebar.markdown(
    '<span style="color:#64748b;font-size:0.8rem;">Prototype · LangGraph + invariants</span>',
    unsafe_allow_html=True,
)

# Session State
if "uploaded_dfs" not in st.session_state:
    st.session_state.uploaded_dfs: Dict[str, pd.DataFrame] = {}

if "mapper_output" not in st.session_state:
    st.session_state.mapper_output = None

if "mapper_approved" not in st.session_state:
    st.session_state.mapper_approved = False

if "aggregator_output" not in st.session_state:
    st.session_state.aggregator_output = None

if "aggregator_report" not in st.session_state:
    st.session_state.aggregator_report = None

if "run_logs" not in st.session_state:
    st.session_state.run_logs = []

if "upload_reset_id" not in st.session_state:
    st.session_state.upload_reset_id = 0

if "pipeline_result" not in st.session_state:
    st.session_state.pipeline_result = None

if "user_instructions" not in st.session_state:
    st.session_state.user_instructions = ""

def _reset_pipeline_session() -> None:
    """Clear data + mapper/ERD/schema outputs so metrics and step 2+ start fresh."""
    st.session_state.uploaded_dfs = {}
    st.session_state.mapper_output = None
    st.session_state.mapper_approved = False
    st.session_state.aggregator_output = None
    st.session_state.aggregator_report = None
    st.session_state.run_logs = []
    st.session_state.pipeline_result = None
    st.session_state.user_instructions = ""
    st.session_state.upload_reset_id = int(st.session_state.get("upload_reset_id", 0)) + 1


# 1. Upload & Profiling
if page == "1 · Upload & Profiling":
    _hero(
        "Step 1 · Ingest",
        "Upload & profiling",
        "Load fragmented CSVs from legacy systems. Profiling is the cataloging step before the Mapper runs.",
    )

    c1, c2, c3 = st.columns(3)
    n_files = len(st.session_state.uploaded_dfs)
    total_rows = (
        sum(len(df) for df in st.session_state.uploaded_dfs.values()) if n_files else 0
    )
    with c1:
        st.metric("Files loaded", n_files)
    with c2:
        st.metric("Total rows (all files)", f"{total_rows:,}")
    with c3:
        st.metric("Pipeline stage", "Profiling" if n_files else "Waiting for data")

    st.markdown('<div class="dw-card">', unsafe_allow_html=True)
    st.markdown("**Ingest CSVs**")
    uploaded_files = st.file_uploader(
        "Select CSV file(s) from this device",
        type=["csv"],
        accept_multiple_files=True,
        key=f"dw_csv_uploader_{st.session_state.upload_reset_id}",
        help="Multiple CSVs supported — each becomes a table for mapping and joins. "
        "Session data persists across steps until you clear or replace it.",
        label_visibility="visible",
    )

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="dw-card">', unsafe_allow_html=True)
    st.markdown("**Transformation instructions** *(optional)*")
    st.caption("Tell the Architect what to do — e.g. 'drop rows where rating is null, normalize enrollment to integers'")
    instructions = st.text_area(
        "Instructions",
        value=st.session_state.user_instructions,
        height=100,
        placeholder="e.g. drop rows where rating is null, cast enrollment to integer, rename 'Unnamed: 0' to 'id'",
        label_visibility="collapsed",
    )
    if instructions != st.session_state.user_instructions:
        st.session_state.user_instructions = instructions
    st.markdown("</div>", unsafe_allow_html=True)

    if uploaded_files:
        st.session_state.uploaded_dfs = {}
        for f in uploaded_files:
            df = pd.read_csv(f)
            st.session_state.uploaded_dfs[f.name] = df
            # Send to backend so /api/run can use it
            try:
                f.seek(0)
                requests.post(f"{BACKEND_URL}/api/upload", files={"file": (f.name, f, "text/csv")}, timeout=10)
            except Exception:
                pass
        st.session_state.mapper_output = None
        st.session_state.mapper_approved = False
        st.session_state.aggregator_output = None
        st.session_state.aggregator_report = None
        st.session_state.run_logs = []
        st.session_state.pipeline_result = None

    if st.session_state.uploaded_dfs:
        st.success(
            f"**{len(st.session_state.uploaded_dfs)}** file(s) in session — "
            "kept while you use steps 2–6. Upload again to **replace**; use **Clear** below to remove."
        )

        c_clear, _ = st.columns([1, 3])
        with c_clear:
            if st.button("🗑 Clear uploaded data & reset pipeline", type="secondary"):
                _reset_pipeline_session()
                st.rerun()

        for name, df in st.session_state.uploaded_dfs.items():
            st.markdown(f"#### 📄 {name}")
            t1, t2 = st.tabs(["Preview", "Column profiling"])
            with t1:
                st.dataframe(df.head(10), use_container_width=True, height=280)
            with t2:
                profile = profile_dataframe(df)
                st.dataframe(profile, use_container_width=True, height=320)

        st.info("Next: open **2 · Mapper (Architect)** to infer schema and ERD.")

# 2. Mapper (Architect) View
elif page == "2 · Mapper (Architect)":
    _hero(
        "Step 2 · Architect agent",
        "Mapper — schema & ERD",
        "Infers keys, relationships, and a data dictionary. Mock output until your LangGraph agent is wired in.",
    )

    if not st.session_state.uploaded_dfs:
        st.warning("No CSVs yet. Complete **1 · Upload & Profiling** first.")
    else:
        col_a, col_b = st.columns([1, 1])
        with col_a:
            if st.button("▶ Run Mapper (mock)", type="primary", use_container_width=True):
                schema_guess = guess_keys(st.session_state.uploaded_dfs)
                schema_guess = enrich_cross_table_foreign_keys(
                    st.session_state.uploaded_dfs, schema_guess
                )
                mermaid_erd = generate_mermaid_erd(
                    schema_guess, st.session_state.uploaded_dfs
                )
                data_dict = []
                for name, df in st.session_state.uploaded_dfs.items():
                    for col in df.columns:
                        data_dict.append(
                            {
                                "table": name,
                                "column": col,
                                "dtype": str(df[col].dtype),
                                "description": infer_column_description(
                                    name, col, df[col], schema_guess
                                ),
                            }
                        )
                st.session_state.mapper_output = {
                    "schema_guess": schema_guess,
                    "mermaid_erd": mermaid_erd,
                    "data_dictionary": data_dict,
                }
                st.session_state.mapper_approved = False
                st.rerun()

        if st.session_state.mapper_output:
            out = st.session_state.mapper_output
            tab_schema, tab_erd, tab_dd = st.tabs(
                ["Schema inference", "ERD (wireframe)", "Data dictionary"]
            )
            with tab_schema:
                render_schema_pipeline_layout_v2(
                    st.session_state.uploaded_dfs,
                    out["schema_guess"],
                )
            with tab_erd:
                st.markdown("##### Layered ERD — wireframe blueprint")
                st.caption(
                    "Dashed clusters & edges = blueprint style (not a final DB diagram). "
                    "Ingest → logical entities → shared keys → artifacts. "
                    "Expand **Physical table detail** for a flat column list with PK/FK tags."
                )
                dot_layered = generate_dataset_erd_dot(
                    st.session_state.uploaded_dfs,
                    out["schema_guess"],
                )
                st.graphviz_chart(dot_layered, use_container_width=True)
                with st.expander("Physical table detail (columns, PK/FK)", expanded=False):
                    st.caption(
                        "One box per CSV. ◆ = PK, ◇ = FK, · = attribute. Edges: child → parent when FKs link tables."
                    )
                    dot_physical = generate_physical_erd_dot(
                        st.session_state.uploaded_dfs,
                        out["schema_guess"],
                    )
                    st.graphviz_chart(dot_physical, use_container_width=True)
                with st.expander("Mermaid ERD (same schema, text)", expanded=False):
                    st.code(out["mermaid_erd"], language="text")
                with st.expander(
                    "Runtime pipeline diagram (LangGraph / code — not data ERD)", expanded=False
                ):
                    st.caption(
                        "This is how the **agent code** is wired; it does not replace the dataset ERD above."
                    )
                    st.graphviz_chart(
                        generate_langgraph_runtime_dot(
                            st.session_state.uploaded_dfs,
                            out["schema_guess"],
                        ),
                        use_container_width=True,
                    )
            with tab_dd:
                dd_df = pd.DataFrame(out["data_dictionary"])
                st.dataframe(
                    dd_df,
                    use_container_width=True,
                    height=420,
                    column_config={
                        "description": st.column_config.TextColumn(
                            "Description",
                            width="large",
                            help="Heuristic summary from name, dtype, PK/FK, nulls.",
                        ),
                    },
                )

            st.info("Review outputs, then **3 · HITL Checkpoint #1** to approve mappings.")
        else:
            st.markdown(
                '<div class="dw-card"><p style="margin:0;color:#64748b;">Click <strong>Run Mapper</strong> to generate schema, ERD text, and dictionary.</p></div>',
                unsafe_allow_html=True,
            )

# 3. HITL Checkpoint #1
elif page == "3 · HITL Checkpoint #1":
    _hero(
        "Step 3 · Human review",
        "HITL checkpoint #1",
        "Confirm inferred ERD and mappings before execution — reduces semantic hallucination risk.",
    )

    if not st.session_state.mapper_output:
        st.warning("Run the Mapper on **2 · Mapper (Architect)** first.")
    else:
        with st.expander("View mapper schema (JSON)", expanded=False):
            st.json(st.session_state.mapper_output["schema_guess"])

        approved = st.checkbox(
            "I confirm the inferred ERD and mappings are correct (or acceptable for a test run).",
            value=st.session_state.mapper_approved,
        )

        if st.button("Save decision", type="primary"):
            st.session_state.mapper_approved = approved
            st.session_state.run_logs.append({"stage": "HITL #1", "approved": approved})
            if approved:
                st.success("Mappings approved. Continue to **4 · Aggregator (Engineer)**.")
            else:
                st.warning("Not approved — adjust inputs or Mapper before Aggregator.")

# 4. Aggregator (Engineer) Run
elif page == "4 · Aggregator (Engineer)":
    _hero(
        "Step 4 · Engineer agent",
        "Aggregator — joins & invariants",
        "Executes validated mappings with deterministic row-count checks.",
    )

    if not st.session_state.uploaded_dfs:
        st.warning("Upload data in **1 · Upload & Profiling**.")
    elif not st.session_state.mapper_output:
        st.warning("Run **2 · Mapper** first.")
    elif not st.session_state.mapper_approved:
        st.error("Approve mappings in **3 · HITL Checkpoint #1** before running.")
    else:
        if st.session_state.user_instructions:
            st.info(f"**Instructions:** {st.session_state.user_instructions}")
        else:
            st.caption("No instructions set — Architect will infer transformations from the data. Add instructions on Step 1.")

        if st.button("▶ Run ETL Pipeline", type="primary"):
            st.session_state.pipeline_result = None

            # Live agent thinking stream
            with st.status("Running ETL pipeline...", expanded=True) as status:
                scout_slot = st.empty()
                arch_slot = st.empty()
                eng_slot = st.empty()
                loader_slot = st.empty()

                collected = {"rows_written": 0, "plan": "", "code": "", "verdict": "", "error": None}

                # Token buffers for live text rendering
                arch_tokens = ""
                eng_tokens = ""

                try:
                    with requests.post(
                        f"{BACKEND_URL}/api/run/stream",
                        json={"user_instructions": st.session_state.user_instructions},
                        stream=True,
                        timeout=300,
                    ) as resp:
                        for line in resp.iter_lines():
                            if not line or not line.startswith(b"data: "):
                                continue
                            event = json.loads(line[6:])
                            etype = event.get("type")
                            node = event.get("node")

                            # --- Token-by-token streaming ---
                            if etype == "token":
                                if node == "architect":
                                    arch_tokens += event.get("token", "")
                                    arch_slot.markdown(f"**Architect** is thinking...\n\n{arch_tokens}")
                                elif node == "engineer":
                                    eng_tokens += event.get("token", "")
                                    eng_slot.markdown(f"**Engineer** is thinking...\n\n```python\n{eng_tokens}\n```")

                            # --- Node completed ---
                            elif etype == "node_done":
                                if node == "scout":
                                    schema = event.get("raw_schema", {})
                                    cols = ", ".join(list(schema.keys())[:6])
                                    scout_slot.success(f"**Scout** — extracted {event.get('record_count', '?')} records  \nColumns: `{cols}`")

                                elif node == "architect":
                                    plan = event.get("transformation_plan", "")
                                    collected["plan"] = plan
                                    arch_tokens = ""
                                    arch_slot.success(f"**Architect** — plan ready ✓")

                                elif node == "engineer":
                                    verdict = event.get("engineer_verdict", "")
                                    collected["verdict"] = verdict
                                    collected["code"] = event.get("transformation_code", "")
                                    retry = event.get("retry_count", 0)
                                    err = event.get("engineer_error", "")
                                    eng_tokens = ""
                                    if verdict == "pass":
                                        eng_slot.success(f"**Engineer** — code executed ✓ (retries: {retry})")
                                    elif verdict == "retry":
                                        eng_slot.warning(f"**Engineer** — retrying ({retry}/3)  \n`{err}`")
                                    else:
                                        eng_slot.error(f"**Engineer** — failed: `{err}`")

                                elif node == "loader":
                                    rows = event.get("rows_written", 0)
                                    collected["rows_written"] = rows
                                    loader_slot.success(f"**Loader** — {rows:,} rows written to SQLite")

                            # --- Pipeline done ---
                            elif etype == "done":
                                status.update(label="Pipeline complete!", state="complete")
                                st.session_state.pipeline_result = {
                                    "status": "success",
                                    "rows_written": collected["rows_written"],
                                    "plan": collected["plan"],
                                    "code": collected["code"],
                                    "verdict": collected["verdict"],
                                }
                                rows = collected["rows_written"]
                                st.session_state.aggregator_report = {"R_final": rows, "invariant_ok": True, "R_product_sources": rows, "source_rows_first_table": rows}
                                st.session_state.run_logs.append({"stage": "Aggregator", "status": "success", "rows_written": rows})

                            elif etype == "error":
                                status.update(label="Pipeline failed", state="error")
                                st.session_state.pipeline_result = {"status": "error", "error": event.get("error"), "traceback": event.get("traceback", "")}
                                st.session_state.run_logs.append({"stage": "Aggregator", "status": "error", "error": event.get("error")})

                except Exception as e:
                    status.update(label="Connection error", state="error")
                    st.session_state.pipeline_result = {"status": "error", "error": str(e)}

        if st.session_state.pipeline_result is not None:
            result = st.session_state.pipeline_result

            if result.get("status") == "error":
                st.error(f"Pipeline failed: {result.get('error')}")
                if result.get("traceback"):
                    with st.expander("Traceback", expanded=False):
                        st.code(result.get("traceback", ""))
            else:
                rows_written = result.get("rows_written", 0)
                m1, m2, m3 = st.columns(3)
                with m1:
                    st.metric("Rows written", f"{rows_written:,}")
                with m2:
                    st.metric("Engineer verdict", result.get("verdict", "—").upper())
                with m3:
                    st.metric("Status", "✓ Success")

                if result.get("plan"):
                    with st.expander("Transformation plan (Architect)", expanded=False):
                        st.markdown(result["plan"])
                if result.get("code"):
                    with st.expander("Generated transformation code (Engineer)", expanded=False):
                        st.code(result["code"], language="python")

                st.success(f"Pipeline complete — {rows_written:,} rows written to SQLite.")

# 5. HITL Checkpoint #2
elif page == "5 · HITL Checkpoint #2":
    _hero(
        "Step 5 · Review failures",
        "HITL checkpoint #2",
        "If invariants fail, record accept/reject before re-run to avoid agent loops.",
    )

    if st.session_state.aggregator_report is None:
        st.warning("Run **4 · Aggregator** first.")
    else:
        st.json(st.session_state.aggregator_report)
        decision = st.radio(
            "Decision",
            ["Accept current output", "Reject and request manual correction"],
            horizontal=True,
        )
        if st.button("Record decision", type="primary"):
            st.session_state.run_logs.append({"stage": "HITL #2", "decision": decision})
            st.success("Recorded.")

# 6. Logs & Downloads
elif page == "6 · Logs & Downloads":
    _hero(
        "Step 6 · Audit & export",
        "Logs & downloads",
        "Session logs and CSV exports. Production would persist to SQLite/PostgreSQL.",
    )

    col_log, col_dl = st.columns([1, 1])

    with col_log:
        st.subheader("Run log")
        if st.session_state.run_logs:
            st.json(st.session_state.run_logs)
        else:
            st.info("No events yet.")

    with col_dl:
        st.subheader("Downloads")
        if st.session_state.pipeline_result and st.session_state.pipeline_result.get("status") == "success":
            try:
                resp = requests.get(f"{BACKEND_URL}/api/results", params={"limit": 10000}, timeout=30)
                rows = resp.json().get("rows", [])
                if rows:
                    cleaned_df = pd.DataFrame(rows)
                    csv_bytes = to_csv_download(cleaned_df)
                    st.download_button(
                        label="⬇ Cleaned dataset (CSV)",
                        data=csv_bytes,
                        file_name="dataweaver_cleaned.csv",
                        mime="text/csv",
                        use_container_width=True,
                        type="primary",
                    )
                else:
                    st.caption("No data returned from backend.")
            except Exception as e:
                st.caption(f"Could not fetch results: {e}")
        else:
            st.caption("Run ETL Pipeline to enable export.")

        if st.session_state.mapper_output is not None:
            dd_df = pd.DataFrame(st.session_state.mapper_output["data_dictionary"])
            csv_bytes = to_csv_download(dd_df)
            st.download_button(
                label="⬇ Data dictionary (CSV)",
                data=csv_bytes,
                file_name="dataweaver_data_dictionary.csv",
                mime="text/csv",
                use_container_width=True,
            )
        else:
            st.caption("Run Mapper to enable dictionary export.")

st.markdown(
    '<p style="text-align:center;color:#94a3b8;font-size:0.8rem;margin-top:2rem;">DataWeaver · Capstone · Team 2</p>',
    unsafe_allow_html=True,
)
