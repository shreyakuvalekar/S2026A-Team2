import html
import io
import os
import re
from typing import Dict, List, Optional, Tuple

import json as _json
import requests
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:5000")


st.set_page_config(page_title="DATA WEAVE", page_icon="🧵", layout="wide")


# -----------------------------------------------------------------------------
# Theme / style (from app_t1)
# -----------------------------------------------------------------------------
def _inject_styles() -> None:
    st.markdown(
        """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700&display=swap');
            @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap');
            html, body, [class*="css"] { font-family: 'DM Sans', system-ui, sans-serif; }
            .block-container { padding-top: 1.35rem; padding-bottom: 1.8rem; max-width: 1220px; }
            [data-testid="stAppViewContainer"] {
                background:
                    radial-gradient(1200px 420px at 8% -10%, rgba(59, 130, 246, 0.16), transparent 45%),
                    radial-gradient(900px 420px at 100% 0%, rgba(14, 116, 144, 0.12), transparent 42%),
                    linear-gradient(180deg, #f8fafc 0%, #f1f5f9 48%, #eef6ff 100%);
            }
            .main .block-container { background: transparent; }

            .dw-hero {
                background: linear-gradient(135deg, #0f2747 0%, #0f172a 45%, #0b4f7a 100%);
                color: #93c5fd;
                padding: 1.1rem 1.3rem;
                border-radius: 12px;
                margin-bottom: 1rem;
                border: 1px solid rgba(125, 211, 252, 0.28);
                box-shadow: 0 12px 24px rgba(15, 23, 42, 0.32);
            }
            .dw-hero h1 { margin: 0; font-size: 1.35rem; font-weight: 700; color: #93c5fd; }
            .dw-hero p { margin: 0.45rem 0 0 0; opacity: 0.95; font-size: 0.92rem; color: #93c5fd; }
            .dw-badge {
                display: inline-block;
                background: rgba(14, 165, 233, 0.24);
                color: #93c5fd;
                padding: 0.18rem 0.55rem;
                border-radius: 6px;
                font-size: 0.72rem;
                font-weight: 600;
                margin-bottom: 0.45rem;
            }

            .dw-card {
                background: #ffffff;
                border: 1px solid #dbeafe;
                border-radius: 10px;
                padding: 0.8rem 1rem;
                margin: 0.75rem 0;
                box-shadow: 0 6px 18px rgba(30, 41, 59, 0.07);
            }
            .dw-topbar {
                width: 100%;
                background: linear-gradient(90deg, #0f2747 0%, #0ea5e9 55%, #22d3ee 100%);
                border-radius: 0;
                padding: 0.28rem 0.75rem;
                margin: 2.2rem -0.25rem 0.65rem -0.25rem;
                position: sticky;
                top: 0;
                z-index: 999;
                box-shadow: 0 1px 6px rgba(2, 132, 199, 0.22);
            }
            .dw-topbar-text {
                color: #ffffff;
                font-size: 0.74rem;
                font-weight: 700;
                letter-spacing: 0.08em;
                text-transform: uppercase;
            }
            .dw-feature-grid {
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 0.7rem;
                margin: 0.35rem 0 0.8rem 0;
            }
            .dw-feature {
                background: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 10px;
                padding: 0.7rem 0.8rem;
                box-shadow: 0 1px 6px rgba(15, 23, 42, 0.05);
            }
            .dw-feature h4 { margin: 0; font-size: 0.87rem; color: #0f172a; }
            .dw-feature p { margin: 0.25rem 0 0 0; font-size: 0.78rem; color: #64748b; line-height: 1.35; }
            .dw-upload-panel {
                background: linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%);
                border: 1px solid #dbeafe;
                border-radius: 12px;
                padding: 0.8rem 0.9rem;
                margin-top: 0.2rem;
            }

            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #0b223f 0%, #102a43 52%, #0f172a 100%);
            }
            [data-testid="stSidebar"] .block-container { padding-top: 1.15rem; }
            [data-testid="stSidebar"] label,
            [data-testid="stSidebar"] p,
            [data-testid="stSidebar"] span { color: #e2e8f0 !important; }
            [data-testid="stSidebar"] h1,
            [data-testid="stSidebar"] h2,
            [data-testid="stSidebar"] h3 {
                color: #93c5fd !important;
            }
            [data-testid="stSidebar"] button[kind="secondary"] {
                border-color: #1d4ed8 !important;
                background: rgba(15, 23, 42, 0.75) !important;
                color: #cbd5e1 !important;
            }
            [data-testid="stSidebar"] button[kind="secondary"]:hover {
                border-color: #2563eb !important;
                background: rgba(15, 39, 71, 0.9) !important;
            }
            [data-testid="stSidebar"] button[kind="primary"] {
                background: linear-gradient(180deg, #1d4ed8, #0369a1) !important;
                border: none !important;
                color: #eff6ff !important;
            }

            .stButton > button[kind="primary"] {
                background: linear-gradient(180deg, #1d4ed8, #0369a1);
                border: none;
                font-weight: 600;
                border-radius: 8px;
                min-height: 2.35rem;
                color: #ffffff;
            }
            .stButton > button[kind="primary"]:hover {
                background: linear-gradient(180deg, #2563eb, #0284c7);
            }
            .stButton > button[kind="secondary"] {
                border-radius: 8px;
                min-height: 2.35rem;
                border: 1px solid #bfdbfe;
                background: #ffffff;
                color: #334155;
            }
            .stButton > button:focus {
                outline: none !important;
                box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.25) !important;
            }

            /* Dashed shell wraps uploader + Add files (see .dw-upload-shell-border) */
            [data-testid="stFileUploader"] section {
                border-radius: 10px;
                border: none;
                background: transparent;
            }
            [data-testid="stMetric"] {
                border: 1px solid #bfdbfe;
                border-radius: 10px;
                background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
                padding: 0.35rem 0.55rem;
                box-shadow: 0 4px 12px rgba(30, 41, 59, 0.06);
            }

            [data-testid="stDataFrame"] {
                border: 1px solid #cbd5e1;
                border-radius: 10px;
                overflow: hidden;
                box-shadow: 0 2px 10px rgba(15, 23, 42, 0.05);
            }

            [data-baseweb="tab-list"] {
                gap: 0.2rem;
                border-bottom: 1px solid #bfdbfe;
                margin-bottom: 0.4rem;
            }
            [data-baseweb="tab"] {
                height: 2rem;
                border-radius: 8px 8px 0 0;
                padding: 0 0.85rem;
                font-weight: 600;
                color: #334155;
            }
            [aria-selected="true"][data-baseweb="tab"] {
                color: #0f2747 !important;
                background: #e6f0ff;
            }

            [data-testid="stAlert"] {
                border-radius: 10px;
                border-width: 1px;
            }

            .stProgress > div > div > div > div {
                background: linear-gradient(90deg, #1d4ed8, #0ea5e9);
            }

            /* Fix: metric text colors */
            [data-testid="stMetricValue"] { color: #0f172a !important; }
            [data-testid="stMetricLabel"] { color: #475569 !important; }

            /* Fix: text input text color in main area */
            .stTextInput input { color: #1e293b !important; }
            .stTextArea textarea {
                color: #1e293b !important;
                background: #ffffff !important;
                border: 1px solid #cbd5e1 !important;
            }
            .stTextArea textarea::placeholder { color: #94a3b8 !important; }

            /* Fix: disabled textarea — Streamlit dims disabled to ~40% opacity */
            .stTextArea textarea:disabled,
            .stTextArea textarea[disabled] {
                color: #334155 !important;
                -webkit-text-fill-color: #334155 !important;
                opacity: 1 !important;
                background: #f8fafc !important;
            }

            /* Fix: caption text */
            .stCaption p, [data-testid="stCaptionContainer"] p { color: #64748b !important; }

            /* Fix: expander summary/header text */
            details > summary { color: #334155 !important; }
            [data-testid="stExpander"] summary p { color: #334155 !important; }

            /* Fix: popover content text (Add files menu) */
            [data-testid="stPopoverBody"] { color: #1e293b !important; }
            [data-testid="stPopoverBody"] p,
            [data-testid="stPopoverBody"] label,
            [data-testid="stPopoverBody"] span { color: #1e293b !important; }

            /* Fix: selectbox, radio, checkbox label text in main area */
            .main .stRadio label, .main .stCheckbox label, .main .stSelectbox label { color: #334155 !important; }
            .main .stRadio p, .main .stCheckbox p, .main .stSelectbox p { color: #334155 !important; }

            /* Fix: file uploader drag-and-drop label text */
            [data-testid="stFileUploaderDropzone"] span,
            [data-testid="stFileUploaderDropzone"] p { color: #475569 !important; }

            /* Hide duplicate native Browse — file picking is via Add files → Browse files */
            [data-testid="stFileUploader"] button[kind="secondary"] {
                display: none !important;
            }

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


def _trigger_native_file_picker() -> None:
    components.html(
        """
        <script>
        const doc = window.parent.document;
        const box = doc.querySelector('[data-testid="stFileUploader"]');
        const inp = box ? box.querySelector('input[type="file"]') : null;
        if (inp) { inp.click(); }
        </script>
        """,
        height=0,
        width=0,
    )


# -----------------------------------------------------------------------------
# Session state
# -----------------------------------------------------------------------------
if "page_idx" not in st.session_state:
    st.session_state.page_idx = 0
if "uploaded_dfs" not in st.session_state:
    st.session_state.uploaded_dfs: Dict[str, pd.DataFrame] = {}
if "source_mode" not in st.session_state:
    st.session_state.source_mode = "Upload csv"
if "trigger_csv_picker" not in st.session_state:
    st.session_state.trigger_csv_picker = False
if "upload_reset_id" not in st.session_state:
    st.session_state.upload_reset_id = 0
if "mapper_schema" not in st.session_state:
    st.session_state.mapper_schema = {}
if "mapper_source_sig" not in st.session_state:
    st.session_state.mapper_source_sig = None
if "data_dictionary" not in st.session_state:
    st.session_state.data_dictionary: List[Dict] = []
if "transformed_df" not in st.session_state:
    st.session_state.transformed_df = None
if "discarded_df" not in st.session_state:
    st.session_state.discarded_df = None
if "transform_source_sig" not in st.session_state:
    st.session_state.transform_source_sig = None
if "run_logs" not in st.session_state:
    st.session_state.run_logs = []
if "agent_dialogue" not in st.session_state:
    st.session_state.agent_dialogue: Dict[str, str] = {
        "Scout": "Waiting for connection...",
        "Architect": "Waiting for connection...",
        "Engineer": "Waiting for connection...",
    }
if "user_instructions" not in st.session_state:
    st.session_state.user_instructions: str = ""
if "pipeline_run_result" not in st.session_state:
    st.session_state.pipeline_run_result: Optional[Dict] = None
if "generated_code_path" not in st.session_state:
    st.session_state.generated_code_path: str = ""
if "hitl_stage" not in st.session_state:
    st.session_state.hitl_stage: str = "idle"   # idle | plan_ready | code_ready | complete
if "hitl_plan" not in st.session_state:
    st.session_state.hitl_plan: str = ""
if "hitl_code" not in st.session_state:
    st.session_state.hitl_code: str = ""
if "hitl_code_path" not in st.session_state:
    st.session_state.hitl_code_path: str = ""
if "hitl_error" not in st.session_state:
    st.session_state.hitl_error: str = ""


PAGES = ["Upload", "Mapper", "Transform", "Logs & Downloads"]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def go_next() -> None:
    st.session_state.page_idx = min(st.session_state.page_idx + 1, len(PAGES) - 1)


def go_back() -> None:
    st.session_state.page_idx = max(st.session_state.page_idx - 1, 0)


def go_to_page(idx: int) -> None:
    st.session_state.page_idx = idx


def _reset_mapper_outputs() -> None:
    st.session_state.mapper_schema = {}
    st.session_state.mapper_source_sig = None
    st.session_state.data_dictionary = []
    st.session_state.transformed_df = None
    st.session_state.discarded_df = None
    st.session_state.transform_source_sig = None


def _reset_hitl() -> None:
    st.session_state.hitl_stage = "idle"
    st.session_state.hitl_plan = ""
    st.session_state.hitl_code = ""
    st.session_state.hitl_code_path = ""
    st.session_state.hitl_error = ""
    st.session_state.agent_dialogue = {
        "Scout": "Waiting for connection...",
        "Architect": "Waiting for connection...",
        "Engineer": "Waiting for connection...",
    }


def _reset_pipeline_session() -> None:
    st.session_state.uploaded_dfs = {}
    _reset_mapper_outputs()
    _reset_hitl()
    st.session_state.run_logs = []
    st.session_state.source_mode = "Upload csv"
    st.session_state.trigger_csv_picker = False
    st.session_state.upload_reset_id = int(st.session_state.get("upload_reset_id", 0)) + 1


def _format_session_csv_size(df: pd.DataFrame) -> str:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    b = len(buf.getvalue().encode("utf-8"))
    if b >= 1024 * 1024:
        return f"{b / (1024 * 1024):.1f} MB"
    if b >= 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b} B"


def _humanize_column_name(col: str) -> str:
    s = str(col).strip()
    if "unnamed" in s.lower():
        return "row_index"
    return s


def _humanize_key_list(cols: List[str]) -> List[str]:
    return [_humanize_column_name(c) for c in cols]


def profile_dataframe(df: pd.DataFrame) -> pd.DataFrame:
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

    return list(dict.fromkeys(relationships))


def infer_column_description(
    table: str,
    col: str,
    series: pd.Series,
    schema_guess: Dict[str, Dict[str, List[str]]],
) -> str:
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


def _erd_table_id(table_name: str) -> str:
    stem = table_name.rsplit(".", 1)[0]
    return "t_" + re.sub(r"[^a-zA-Z0-9]", "_", stem)


def _assign_columns_to_logical_entities(
    df: pd.DataFrame, keys: Dict[str, List[str]]
) -> List[Tuple[str, List[str]]]:
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

    rest = [c for c in cols if c not in assigned]
    if rest:
        groups.append(("ENTITY · ATTRIBUTES (remaining)", rest))

    while len(groups) < 4 and any(len(g[1]) > 2 for g in groups):
        for i, (nm, gcols) in enumerate(groups):
            if len(gcols) <= 2:
                continue
            mid = len(gcols) // 2
            a, b = gcols[:mid], gcols[mid:]
            groups[i] = (nm + " (a)", a)
            groups.insert(i + 1, (nm + " (b)", b))
            break

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


def generate_dataset_erd_dot(
    dfs: Dict[str, pd.DataFrame],
    schema_guess: Dict[str, Dict[str, List[str]]],
) -> str:
    if not dfs:
        return 'digraph X { label="No datasets"; bgcolor="#f8fafc"; }'

    rels = infer_relationships(schema_guess)
    total_rows = sum(len(df) for df in dfs.values())
    total_cols = sum(len(df.columns) for df in dfs.values())

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

    for parent, child, fk in rels:
        pid = _erd_table_id(parent)
        cid = _erd_table_id(child)
        fk_esc = fk.replace('"', '\\"')
        lines.append(
            f'  {pid} -> {cid} [label="{fk_esc} (FK)", color="#2563eb", penwidth=1.1, arrowsize=0.9, '
            f'constraint=false];'
        )

    for fname in dfs:
        tid = _erd_table_id(fname)
        for j in range(len(ent_ids)):
            lines.append(
                f'  {tid} -> ent_{j} [label="columns", color="#64748b", penwidth=1.0];'
            )

    for j, eid in enumerate(ent_ids):
        lines.append(
            f'  {eid} -> shared_keys [label="infer keys", color="#7c3aed", penwidth=1.05];'
        )

    lines.extend(
        [
            '  shared_keys -> art_dict [label="feeds", color="#b45309", penwidth=1.1];',
            '  shared_keys -> art_mermaid [label="feeds", color="#b45309", penwidth=1.1];',
            '  shared_keys -> art_target [label="feeds", color="#b45309", penwidth=1.1];',
            "",
        ]
    )

    for j in range(len(ent_ids) - 1):
        lines.append(
            f'  ent_{j} -> ent_{j + 1} [label="1:1 via row", color="#16a34a", '
            f'constraint=false, fontsize=7, penwidth=0.9];'
        )

    lines.append("}")
    return "\n".join(lines)


def _quality_tag(df: pd.DataFrame) -> str:
    if df.empty:
        return "Needs review"
    null_ratio = float(df.isna().sum().sum()) / float(max(1, df.shape[0] * max(1, df.shape[1])))
    if null_ratio < 0.03:
        return "Ready"
    if null_ratio < 0.12:
        return "Watch"
    return "Needs review"


def planned_transforms_named(df: pd.DataFrame) -> List[str]:
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
    return re.sub(r"\*\*(.+?)\*\*", r"\1", s)


def render_schema_pipeline_layout_v2(
    dfs: Dict[str, pd.DataFrame],
    schema_guess: Optional[Dict[str, Dict[str, List[str]]]] = None,
) -> None:
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
  <div class="dw-wf-meta">Confirm in <strong>Transform</strong> (step 3) before running guided transforms.</div>
  <span class="dw-wf-pill dw-wf-pill-ok">Ready for review → Transform</span>
</div>
"""
    with lane_t:
        st.markdown(
            '<div class="dw-wf-lane-h t">Transform · inference & plan</div>',
            unsafe_allow_html=True,
        )
        st.markdown(transform_html, unsafe_allow_html=True)

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


def make_data_dictionary(dfs: Dict[str, pd.DataFrame], schema: Dict[str, Dict[str, List[str]]]) -> List[Dict]:
    rows: List[Dict] = []
    for table, df in dfs.items():
        keys = schema.get(table, {"primary_keys": [], "foreign_keys": []})
        for col in df.columns:
            role = "attribute"
            if col in keys["primary_keys"]:
                role = "primary_key"
            elif col in keys["foreign_keys"]:
                role = "foreign_key"
            rows.append(
                {
                    "table": table,
                    "column": col,
                    "dtype": str(df[col].dtype),
                    "role": role,
                    "null_pct": round(df[col].isna().mean() * 100, 2),
                    "description": infer_column_description(table, str(col), df[col], schema),
                }
            )
    return rows


def run_transform(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    if not dfs:
        return pd.DataFrame()
    items = list(dfs.items())
    base = items[0][1].copy()
    for _, df in items[1:]:
        common = list(set(base.columns) & set(df.columns))
        keys = [c for c in common if str(c).lower() == "id" or str(c).lower().endswith("_id")]
        if keys:
            base = base.merge(df, on=keys[0], how="left", suffixes=("", "_r"))
    return base


_AGENT_CARD_STYLES = {
    "Scout":    ("#dbeafe", "#1d4ed8", "#1e3a5f"),
    "Architect": ("#fef3c7", "#b45309", "#451a03"),
    "Engineer": ("#dcfce7", "#15803d", "#052e16"),
}


def _agent_dialogue_html(dialogue: Dict[str, str]) -> str:
    cards = []
    for agent, msg in dialogue.items():
        bg, accent, text_col = _AGENT_CARD_STYLES.get(agent, ("#f1f5f9", "#475569", "#0f172a"))
        escaped = html.escape(str(msg)).replace("\n", "<br>")
        cards.append(
            f'<div style="background:{bg};border-left:3px solid {accent};border-radius:6px;'
            f'padding:10px 14px;margin-bottom:8px;">'
            f'<div style="font-size:0.7rem;font-weight:700;color:{accent};'
            f'letter-spacing:0.09em;text-transform:uppercase;margin-bottom:4px;">{agent}</div>'
            f'<div style="font-size:0.82rem;color:{text_col};line-height:1.55;'
            f'font-family:\'IBM Plex Mono\',monospace;word-break:break-word;">{escaped}</div>'
            f'</div>'
        )
    return (
        '<div style="border:1px solid #e2e8f0;border-radius:8px;background:#f8fafc;padding:10px 12px;">'
        + "".join(cards)
        + "</div>"
    )


def render_agent_dialogue_box(placeholder: Optional[object] = None) -> None:
    st.markdown("**Agent dialogue**")
    st.caption("Live status feed from agents (updates when backend is connected).")
    target = placeholder if placeholder is not None else st
    target.markdown(_agent_dialogue_html(st.session_state.agent_dialogue), unsafe_allow_html=True)


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

# -----------------------------------------------------------------------------
# Header + stepper
# -----------------------------------------------------------------------------
_inject_styles()

st.sidebar.title("Navigation")
for idx, page_name in enumerate(PAGES):
    label = f"{idx + 1}. {page_name}"
    st.sidebar.button(
        label,
        key=f"sidebar_step_{idx}",
        on_click=go_to_page,
        args=(idx,),
        use_container_width=True,
        type="primary" if st.session_state.page_idx == idx else "secondary",
    )

st.sidebar.markdown("---")
st.sidebar.caption(f"Current step: {st.session_state.page_idx + 1} / {len(PAGES)}")

current = PAGES[st.session_state.page_idx]

st.markdown(
    """
    <div class="dw-topbar">
        <div class="dw-topbar-text">DATA WEAVE</div>
    </div>
    """,
    unsafe_allow_html=True,
)


# -----------------------------------------------------------------------------
# Page 1: Upload / dashboard
# -----------------------------------------------------------------------------
if current == "Upload":
    _hero("Step 1", "Upload", "Upload csv, URL API, or Google Drive source")
    mc1, mc2, mc3 = st.columns(3)
    with mc1:
        st.metric("Sources in session", len(st.session_state.uploaded_dfs))
    with mc2:
        total_rows = sum(len(df) for df in st.session_state.uploaded_dfs.values()) if st.session_state.uploaded_dfs else 0
        st.metric("Rows available", f"{total_rows:,}")
    with mc3:
        st.metric("Pipeline step", "Ingest")

    mode = st.session_state.source_mode

    def _add_files_menu() -> None:
        with st.popover("Add files", use_container_width=False):
            st.caption("Select how to add data")
            if st.button("Browse files", key="add_menu_browse", use_container_width=True):
                st.session_state.source_mode = "Upload csv"
                st.session_state.trigger_csv_picker = True
                st.rerun()
            if st.button("Upload url api", key="add_menu_api", use_container_width=True):
                st.session_state.source_mode = "Upload url api"
                st.rerun()
            if st.button("Connect google drive", key="add_menu_drive", use_container_width=True):
                st.session_state.source_mode = "Connect google drive"
                st.rerun()

    st.markdown(
        """
        <style>
        section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] {
            border: 2px dotted #60a5fa !important;
            border-style: dotted !important;
            border-radius: 10px !important;
            background: #f8fafc !important;
            padding: 0.55rem 0.6rem 0.75rem !important;
            margin-bottom: 0.35rem !important;
            box-shadow: none !important;
        }
        section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] div[data-testid="column"]:nth-child(1),
        section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stHorizontalBlock"] > div[data-testid="column"]:first-child {
            background: linear-gradient(180deg, #dbeafe 0%, #bfdbfe 42%, #e0f2fe 100%) !important;
            border-radius: 8px !important;
            padding: 0.5rem 0.65rem 0.6rem !important;
            margin: 0.1rem 0.15rem 0.1rem 0.1rem !important;
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.65) !important;
        }
        section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] div[data-testid="column"]:nth-child(2),
        section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child {
            background: #ffffff !important;
            border-radius: 8px !important;
            padding: 0.35rem 0.4rem !important;
            margin: 0.1rem 0.1rem 0.1rem 0 !important;
            box-shadow: 0 1px 4px rgba(15, 23, 42, 0.06) !important;
            align-self: flex-start !important;
        }
        section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stFileUploader"] section {
            background: transparent !important;
        }
        section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] .stTextInput input {
            border-radius: 10px !important;
            border: 1px solid #e2e8f0 !important;
            background: #f8fafc !important;
            padding: 0.55rem 0.65rem !important;
        }
        section[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] .stTextInput label p {
            font-size: 0.72rem !important;
            font-weight: 700 !important;
            letter-spacing: 0.04em !important;
            color: #64748b !important;
            text-transform: uppercase;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    files = None
    with st.container(border=True):
        row = st.columns([4, 1])
        with row[0]:
            if mode == "Upload csv":
                st.caption("Select CSV file(s) from this device")
                files = st.file_uploader(
                    "csv_upload",
                    type=["csv"],
                    accept_multiple_files=True,
                    label_visibility="collapsed",
                    key=f"dw_csv_uploader_{st.session_state.upload_reset_id}",
                )
                if st.session_state.uploaded_dfs and not files:
                    st.markdown("---")
                    st.markdown("**Files in session**")
                    for fname, df in st.session_state.uploaded_dfs.items():
                        sz = _format_session_csv_size(df)
                        st.markdown(f"- 📄 `{fname}` · **{sz}** · {len(df):,} rows")
            elif mode == "Upload url api":
                st.caption("Paste your API endpoint URL")
                st.text_input(
                    "API URL",
                    key="upload_api_url_field",
                    placeholder="https://api.example.com/data",
                )
            else:
                st.caption("Paste a Google Drive file or folder link")
                st.text_input(
                    "Google Drive file/folder link",
                    key="upload_drive_link_field",
                    placeholder="https://drive.google.com/file/d/...",
                )
                st.link_button("Open Google Drive", "https://drive.google.com/", use_container_width=True)
        with row[1]:
            st.markdown("<br/>", unsafe_allow_html=True)
            _add_files_menu()
        if mode == "Upload url api":
            if st.button("Submit API URL", key="upload_submit_api", use_container_width=True):
                _reset_mapper_outputs()
                st.session_state.run_logs = []
                st.session_state.uploaded_dfs = {
                    "api_data.csv": pd.DataFrame({"id": [1, 2], "value": [100, 200]})
                }
        elif mode == "Connect google drive":
            if st.button("Use Google Drive link", key="upload_submit_drive", use_container_width=True):
                _reset_mapper_outputs()
                st.session_state.run_logs = []
                st.session_state.uploaded_dfs = {
                    "drive_data.csv": pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
                }

    if mode == "Upload csv":
        if st.session_state.trigger_csv_picker:
            _trigger_native_file_picker()
            st.session_state.trigger_csv_picker = False
        if files:
            _reset_mapper_outputs()
            st.session_state.run_logs = []
            st.session_state.uploaded_dfs = {}
            for f in files:
                st.session_state.uploaded_dfs[f.name] = pd.read_csv(f)

    if st.session_state.uploaded_dfs:
        n = len(st.session_state.uploaded_dfs)
        st.success(
            f"**{n}** file(s) in session — kept while you use steps 2–4. "
            "Upload again to **replace**; use **Clear** below to remove."
        )
        c_clear, _ = st.columns([1, 3])
        with c_clear:
            if st.button("🗑 Clear uploaded data & reset pipeline", type="secondary"):
                _reset_pipeline_session()
                st.rerun()
        st.caption("Data source is ready. Click **Next** to continue to Mapper.")

    c1, c2, _ = st.columns([1, 1, 6])
    with c1:
        st.button("Back", disabled=True)
    with c2:
        st.button("Next", on_click=go_next, type="primary")


# -----------------------------------------------------------------------------
# Page 2: Mapper
# -----------------------------------------------------------------------------
elif current == "Mapper":
    _hero("Step 2", "Mapper", "Preview, schema inference, ERD, and data dictionary")

    if not st.session_state.uploaded_dfs:
        st.warning("No uploaded data found. Go back to Upload page.")
    else:
        mk1, mk2, mk3 = st.columns(3)
        with mk1:
            st.metric("Tables", len(st.session_state.uploaded_dfs))
        with mk2:
            st.metric("Total fields", sum(len(df.columns) for df in st.session_state.uploaded_dfs.values()))
        with mk3:
            st.metric("Step status", "Auto-mapped")

        source_sig = tuple(
            sorted((name, len(df), len(df.columns)) for name, df in st.session_state.uploaded_dfs.items())
        )
        if st.session_state.mapper_source_sig != source_sig:
            schema_guess = guess_keys(st.session_state.uploaded_dfs)
            schema = enrich_cross_table_foreign_keys(st.session_state.uploaded_dfs, schema_guess)
            st.session_state.mapper_schema = schema
            st.session_state.data_dictionary = make_data_dictionary(st.session_state.uploaded_dfs, schema)
            st.session_state.mapper_source_sig = source_sig
            st.session_state.run_logs.append({"stage": "Mapper", "status": "completed"})

        t1, t2, t3, t4, t5 = st.tabs(
            ["Preview", "Column profiling", "Schema inference", "ERD", "DATA DICTIONARY"]
        )

        with t1:
            for name, df in st.session_state.uploaded_dfs.items():
                st.markdown(f"### 📄 {name}")
                st.dataframe(df.head(10), use_container_width=True, height=300)
        with t2:
            for name, df in st.session_state.uploaded_dfs.items():
                st.markdown(f"### 📄 {name}")
                st.dataframe(profile_dataframe(df), use_container_width=True, height=340)
        with t3:
            if st.session_state.mapper_schema:
                render_schema_pipeline_layout_v2(
                    st.session_state.uploaded_dfs,
                    st.session_state.mapper_schema,
                )
                with st.expander("Raw schema JSON", expanded=False):
                    st.json(st.session_state.mapper_schema)
                st.markdown("**Inferred relationships**")
                rels = infer_relationships(st.session_state.mapper_schema)
                st.dataframe(
                    pd.DataFrame(rels, columns=["parent_table", "child_table", "fk_column"])
                    if rels
                    else pd.DataFrame(columns=["parent_table", "child_table", "fk_column"]),
                    use_container_width=True,
                )
            else:
                st.info("Schema inference not available.")
        with t4:
            if st.session_state.mapper_schema:
                st.markdown("##### Layered ERD — wireframe blueprint")
                st.caption(
                    "Dashed clusters & edges = blueprint style (not a final DB diagram). "
                    "Ingest → logical entities → shared keys → artifacts."
                )
                dot_layered = generate_dataset_erd_dot(
                    st.session_state.uploaded_dfs,
                    st.session_state.mapper_schema,
                )
                st.graphviz_chart(dot_layered, use_container_width=True)
            else:
                st.info("ERD not available.")
        with t5:
            if st.session_state.data_dictionary:
                st.dataframe(
                    pd.DataFrame(st.session_state.data_dictionary),
                    use_container_width=True,
                    column_config={
                        "description": st.column_config.TextColumn(
                            "Description",
                            width="large",
                            help="Heuristic summary from name, dtype, PK/FK, nulls.",
                        ),
                    },
                )
                st.info("Review outputs, then continue to **Transform** (step 3).")
            else:
                st.info("Data dictionary not available.")

    c1, c2, _ = st.columns([1, 1, 6])
    with c1:
        st.button("Back", on_click=go_back)
    with c2:
        st.button("Next", on_click=go_next, type="primary")


# -----------------------------------------------------------------------------
# Page 3: Transform
# -----------------------------------------------------------------------------
elif current == "Transform":
    _hero("Step 3", "Transform", "Human-in-the-loop: review plan → review code → execute")

    if not st.session_state.uploaded_dfs:
        st.warning("No uploaded data found. Go back to Upload.")
    else:
        stage = st.session_state.hitl_stage

        tk1, tk2, tk3 = st.columns(3)
        with tk1:
            st.metric("Source tables", len(st.session_state.uploaded_dfs))
        with tk2:
            n_rows = len(st.session_state.transformed_df) if st.session_state.transformed_df is not None else 0
            st.metric("Transformed rows", n_rows)
        with tk3:
            _stage_labels = {
                "idle": "Ready",
                "plan_ready": "Review plan",
                "code_ready": "Review code",
                "complete": "Done",
            }
            st.metric("Stage", _stage_labels.get(stage, stage))

        # Persistent error banner — survives reruns
        if st.session_state.hitl_error:
            st.error(st.session_state.hitl_error)
            if st.button("Dismiss error", key="dismiss_hitl_error"):
                st.session_state.hitl_error = ""
                st.rerun()

        t1, t2, t3 = st.tabs(["Transform", "Discarded Data", "Transformed data"])

        with t1:
            # ── Pipeline step progress bar ───────────────────────────────────────
            _step_names = ["Instructions", "Approve plan", "Approve code", "Complete"]
            _stage_to_step = {"idle": 0, "plan_ready": 1, "code_ready": 2, "complete": 3}
            _active = _stage_to_step.get(stage, 0)
            _step_cols = st.columns(4)
            for _si, (_sc, _sn) in enumerate(zip(_step_cols, _step_names)):
                with _sc:
                    if _si < _active:
                        st.markdown(
                            f'<div style="text-align:center;padding:6px 4px;border-radius:6px;'
                            f'background:#d1fae5;color:#065f46;font-size:0.78rem;font-weight:600;">'
                            f'&#10003; {_sn}</div>',
                            unsafe_allow_html=True,
                        )
                    elif _si == _active:
                        st.markdown(
                            f'<div style="text-align:center;padding:6px 4px;border-radius:6px;'
                            f'background:#dbeafe;color:#1e40af;font-size:0.78rem;font-weight:700;'
                            f'border:2px solid #3b82f6;">'
                            f'&#9654; {_sn}</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            f'<div style="text-align:center;padding:6px 4px;border-radius:6px;'
                            f'background:#f1f5f9;color:#94a3b8;font-size:0.78rem;">'
                            f'{_sn}</div>',
                            unsafe_allow_html=True,
                        )
            st.markdown("<div style='margin-top:0.75rem'></div>", unsafe_allow_html=True)

            # ── STEP 1: idle — instructions + run ───────────────────────────────
            if stage == "idle":
                with st.container(border=True):
                    st.markdown("**Step 1 — Instructions**")
                    st.caption("Describe the transformation rules, then click Run to start the pipeline. Scout and Architect agents will run automatically.")
                    user_instructions = st.text_area(
                        "Instructions for the pipeline",
                        value=st.session_state.user_instructions,
                        placeholder=(
                            "e.g. Drop rows where revenue is null. "
                            "Normalize date columns to ISO 8601. "
                            "Rename 'cust_id' to 'customer_id'."
                        ),
                        height=100,
                        key="user_instructions_input",
                        label_visibility="collapsed",
                    )
                    st.session_state.user_instructions = user_instructions

                    run_col, _ = st.columns([1, 3])
                    with run_col:
                        run_clicked = st.button("Run pipeline", type="primary", key="run_pipeline_btn", use_container_width=True)

                if run_clicked:
                    if not st.session_state.uploaded_dfs:
                        st.session_state.hitl_error = "No file uploaded. Go to the Upload step first."
                        st.rerun()
                    else:
                        source_name = next(iter(st.session_state.uploaded_dfs))
                        source_df = st.session_state.uploaded_dfs[source_name]

                        upload_error = None
                        saved_path = f"datasets/{source_name}"
                        try:
                            upload_resp = requests.post(
                                f"{BACKEND_URL}/api/upload",
                                files={"file": (source_name, to_csv_bytes(source_df), "text/csv")},
                                timeout=30,
                            )
                            if upload_resp.status_code == 200:
                                saved_path = upload_resp.json().get("saved_path", saved_path)
                            else:
                                upload_error = upload_resp.json().get("error", "Upload failed")
                        except requests.exceptions.ConnectionError:
                            upload_error = f"Cannot reach backend at {BACKEND_URL}. Is the API server running?"
                        except Exception as exc:
                            upload_error = f"Upload error: {exc}"

                        if upload_error:
                            st.session_state.hitl_error = upload_error
                            st.rerun()
                        else:
                            table_name = source_name.rsplit(".", 1)[0].lower().replace(" ", "_").replace("-", "_")
                            payload = {
                                "source_type": "csv",
                                "source_path": saved_path,
                                "user_instructions": st.session_state.user_instructions,
                                "target_db_path": "output/etl_output.db",
                                "target_table": table_name,
                                "if_exists": "replace",
                            }

                            streaming_dialogue: Dict[str, str] = {
                                "Scout": "Connecting…",
                                "Architect": "Waiting…",
                                "Engineer": "Waiting…",
                            }
                            st.session_state.agent_dialogue = streaming_dialogue
                            with st.status("Running Scout + Architect…", expanded=True) as run_status:
                                st.write("Uploading data and starting pipeline…")
                                token_buf: Dict[str, str] = {"architect": ""}
                                plan_preview = st.empty()

                                try:
                                    with requests.post(
                                        f"{BACKEND_URL}/api/run/stream/plan",
                                        json=payload, stream=True, timeout=300,
                                    ) as resp:
                                        for raw_line in resp.iter_lines():
                                            if not raw_line:
                                                continue
                                            line = raw_line.decode("utf-8")
                                            if not line.startswith("data: "):
                                                continue
                                            try:
                                                event = _json.loads(line[6:])
                                            except _json.JSONDecodeError:
                                                continue

                                            etype = event.get("type", "")
                                            node = event.get("node", "")

                                            if etype == "token" and node == "architect":
                                                token_buf["architect"] += event.get("token", "")
                                                plan_preview.markdown(token_buf["architect"])

                                            elif etype == "node_done":
                                                if node == "scout":
                                                    st.write(f"Scout done — {event.get('record_count', '?')} records ingested.")
                                                    streaming_dialogue["Scout"] = f"Done — {event.get('record_count', '?')} records ingested."
                                                elif node == "architect":
                                                    st.write("Architect done — plan ready for review.")
                                                    streaming_dialogue["Architect"] = event.get("transformation_plan", "").strip() or "Plan generated."

                                            elif etype == "plan_ready":
                                                st.session_state.hitl_plan = event.get("transformation_plan", "")
                                                st.session_state.hitl_stage = "plan_ready"
                                                st.session_state.hitl_error = ""
                                                st.session_state.agent_dialogue = streaming_dialogue
                                                st.session_state.run_logs.append({"stage": "Phase 1 — plan", "status": "ready for review"})
                                                run_status.update(label="Architect plan ready — awaiting your approval", state="complete")
                                                break

                                            elif etype == "error":
                                                st.session_state.hitl_error = f"Pipeline error (Phase 1): {event.get('error', 'unknown')}"
                                                run_status.update(label="Pipeline error", state="error")
                                                break

                                except requests.exceptions.ConnectionError:
                                    st.session_state.hitl_error = f"Cannot reach backend at {BACKEND_URL}. Is the API server running?"
                                except Exception as exc:
                                    st.session_state.hitl_error = f"Unexpected error: {exc}"

                            st.rerun()

            # ── STEP 2: plan_ready — Architect plan approval ─────────────────────
            elif stage == "plan_ready":
                st.markdown(
                    '<div style="padding:10px 14px;border-radius:8px;background:#fef9c3;'
                    'border:1px solid #fbbf24;color:#713f12;font-weight:600;margin-bottom:0.75rem;">'
                    '&#9203; Waiting for your approval — review the Architect\'s plan below</div>',
                    unsafe_allow_html=True,
                )

                with st.container(border=True):
                    st.markdown("**Step 2 — Approve transformation plan**")
                    st.caption("The Architect mapped out how the data will be transformed. Edit the plan if needed, then approve to send it to the Engineer for code generation.")

                    edited_plan = st.text_area(
                        "Transformation plan",
                        value=st.session_state.hitl_plan,
                        height=320,
                        key="hitl_plan_editor",
                        label_visibility="collapsed",
                    )

                    approve_col, reset_col = st.columns([3, 1])
                    with approve_col:
                        approve_plan = st.button(
                            "Approve plan — generate code",
                            type="primary",
                            key="approve_plan_btn",
                            use_container_width=True,
                        )
                    with reset_col:
                        if st.button("Start over", key="reset_from_plan_btn", use_container_width=True):
                            _reset_hitl()
                            st.rerun()

                if approve_plan:
                    st.session_state.hitl_plan = edited_plan
                    streaming_dialogue = dict(st.session_state.agent_dialogue)
                    streaming_dialogue["Engineer"] = "Generating code…"

                    with st.status("Engineer generating transformation code…", expanded=True) as gen_status:
                        token_buf = {"engineer_generate": ""}
                        code_preview = st.empty()

                        try:
                            with requests.post(
                                f"{BACKEND_URL}/api/run/stream/generate",
                                json={"transformation_plan": edited_plan},
                                stream=True, timeout=300,
                            ) as resp:
                                for raw_line in resp.iter_lines():
                                    if not raw_line:
                                        continue
                                    line = raw_line.decode("utf-8")
                                    if not line.startswith("data: "):
                                        continue
                                    try:
                                        event = _json.loads(line[6:])
                                    except _json.JSONDecodeError:
                                        continue

                                    etype = event.get("type", "")
                                    node = event.get("node", "")

                                    if etype == "token" and node == "engineer_generate":
                                        token_buf["engineer_generate"] += event.get("token", "")
                                        streaming_dialogue["Engineer"] = token_buf["engineer_generate"]
                                        code_preview.code(token_buf["engineer_generate"], language="python")

                                    elif etype == "node_done" and node == "engineer_generate":
                                        n_lines = len(event.get("transformation_code", "").splitlines())
                                        st.write(f"Code generated — {n_lines} lines. Ready for review.")
                                        streaming_dialogue["Engineer"] = f"Code generated ({n_lines} lines). Ready for review."

                                    elif etype == "code_ready":
                                        st.session_state.hitl_code = event.get("transformation_code", "")
                                        st.session_state.hitl_code_path = event.get("generated_code_path", "")
                                        st.session_state.generated_code_path = event.get("generated_code_path", "")
                                        st.session_state.hitl_stage = "code_ready"
                                        st.session_state.hitl_error = ""
                                        st.session_state.agent_dialogue = streaming_dialogue
                                        st.session_state.run_logs.append({"stage": "Phase 2a — generate", "status": "ready for review"})
                                        gen_status.update(label="Code ready — awaiting your approval", state="complete")
                                        break

                                    elif etype == "error":
                                        st.session_state.hitl_error = f"Pipeline error (Phase 2a): {event.get('error', 'unknown')}"
                                        gen_status.update(label="Code generation error", state="error")
                                        break

                        except requests.exceptions.ConnectionError:
                            st.session_state.hitl_error = f"Cannot reach backend at {BACKEND_URL}. Is the API server running?"
                        except Exception as exc:
                            st.session_state.hitl_error = f"Unexpected error: {exc}"

                    st.rerun()

            # ── STEP 3: code_ready — Engineer code approval ──────────────────────
            elif stage == "code_ready":
                st.markdown(
                    '<div style="padding:10px 14px;border-radius:8px;background:#fef9c3;'
                    'border:1px solid #fbbf24;color:#713f12;font-weight:600;margin-bottom:0.75rem;">'
                    '&#9203; Waiting for your approval — review the Engineer\'s code below</div>',
                    unsafe_allow_html=True,
                )

                with st.container(border=True):
                    st.markdown("**Step 3 — Approve transformation code**")
                    st.caption("The Engineer wrote this Python script to execute the plan. Download it to inspect locally, then approve to run it.")

                    st.code(st.session_state.hitl_code, language="python")

                    dl_col, approve_col, reset_col = st.columns([2, 2, 1])
                    with dl_col:
                        st.download_button(
                            label="Download .py",
                            data=st.session_state.hitl_code.encode("utf-8"),
                            file_name="engineer_transformation.py",
                            mime="text/x-python",
                            use_container_width=True,
                        )
                    with approve_col:
                        approve_code = st.button(
                            "Approve & execute",
                            type="primary",
                            key="approve_code_btn",
                            use_container_width=True,
                        )
                    with reset_col:
                        if st.button("Start over", key="reset_from_code_btn", use_container_width=True):
                            _reset_hitl()
                            st.rerun()

                if approve_code:
                    streaming_dialogue = dict(st.session_state.agent_dialogue)
                    streaming_dialogue["Engineer"] = "Executing approved code…"

                    with st.status("Executing transformation…", expanded=True) as exec_status:
                        try:
                            with requests.post(
                                f"{BACKEND_URL}/api/run/stream/run",
                                json={"transformation_code": st.session_state.hitl_code},
                                stream=True, timeout=300,
                            ) as resp:
                                for raw_line in resp.iter_lines():
                                    if not raw_line:
                                        continue
                                    line = raw_line.decode("utf-8")
                                    if not line.startswith("data: "):
                                        continue
                                    try:
                                        event = _json.loads(line[6:])
                                    except _json.JSONDecodeError:
                                        continue

                                    etype = event.get("type", "")
                                    node = event.get("node", "")

                                    if etype == "token" and node == "engineer_generate":
                                        streaming_dialogue["Engineer"] = "Auto-retry: regenerating code…"
                                        st.write("Execution failed — auto-retrying with new code…")

                                    elif etype == "node_done":
                                        if node == "engineer_generate":
                                            new_code = event.get("transformation_code", "")
                                            if new_code:
                                                st.session_state.hitl_code = new_code
                                                st.session_state.generated_code_path = event.get("generated_code_path", "")
                                            streaming_dialogue["Engineer"] = "Code regenerated (retry). Executing…"
                                            st.write("New code generated. Executing…")
                                        elif node == "engineer_execute":
                                            verdict = event.get("engineer_verdict", "")
                                            err = event.get("engineer_error", "")
                                            streaming_dialogue["Engineer"] = (
                                                f"Verdict: {verdict}" + (f"\nError: {err}" if err else "")
                                            )
                                            st.write(f"Execute verdict: {verdict}" + (f" — {err}" if err else ""))
                                            transformed_records = event.get("transformed_data")
                                            if transformed_records and verdict == "pass":
                                                st.session_state.transformed_df = pd.DataFrame(transformed_records)
                                        elif node == "loader":
                                            rows = event.get("rows_written", "?")
                                            streaming_dialogue["Scout"] = (
                                                st.session_state.agent_dialogue.get("Scout", "") + f"\nLoaded: {rows} rows to DB."
                                            )
                                            st.write(f"Loader done — {rows} rows written to database.")

                                    elif etype == "done":
                                        st.session_state.hitl_stage = "complete"
                                        st.session_state.agent_dialogue = streaming_dialogue
                                        st.session_state.pipeline_run_result = {"status": "success"}
                                        st.session_state.run_logs.append({"stage": "Pipeline run", "status": "success"})
                                        exec_status.update(label="Pipeline complete!", state="complete")
                                        break

                                    elif etype == "error":
                                        st.session_state.hitl_error = f"Pipeline error: {event.get('error', 'unknown')}"
                                        exec_status.update(label="Execution error", state="error")
                                        break

                        except requests.exceptions.ConnectionError:
                            st.session_state.hitl_error = f"Cannot reach backend at {BACKEND_URL}. Is the API server running?"
                        except Exception as exc:
                            st.session_state.hitl_error = f"Unexpected error: {exc}"

                    st.rerun()

            # ── STEP 4: complete ─────────────────────────────────────────────────
            elif stage == "complete":
                st.success("Pipeline complete — data transformed and loaded.")
                if st.session_state.transformed_df is not None:
                    st.markdown("**Transformed data preview**")
                    st.dataframe(st.session_state.transformed_df.head(20), use_container_width=True)
                    st.caption("Go to the **Transformed data** tab or **Logs & Downloads** for exports and the full audit trail.")
                c_again, _ = st.columns([1, 4])
                with c_again:
                    if st.button("Run again", type="secondary", key="run_again_btn"):
                        _reset_hitl()
                        st.rerun()

            # Always show agent dialogue
            st.markdown("---")
            st.markdown("**Agent dialogue**")
            st.caption("Live status feed from agents.")
            st.markdown(_agent_dialogue_html(st.session_state.agent_dialogue), unsafe_allow_html=True)

        with t2:
            st.markdown("##### Discarded Data")
            discarded = st.session_state.discarded_df
            if discarded is not None and not discarded.empty:
                st.dataframe(discarded, use_container_width=True, height=520)
                st.caption(f"{len(discarded):,} rows · {len(discarded.columns)} columns")
            else:
                st.info(
                    "No discarded rows are in session yet. When the transform pipeline records "
                    "dropped or rejected records, they will show here."
                )
        with t3:
            st.markdown("##### Transformed data")
            if st.session_state.transformed_df is not None and not st.session_state.transformed_df.empty:
                st.dataframe(
                    st.session_state.transformed_df,
                    use_container_width=True,
                    height=520,
                )
                st.caption(
                    f"{len(st.session_state.transformed_df):,} rows · "
                    f"{len(st.session_state.transformed_df.columns)} columns"
                )
            else:
                st.info("No transformed table yet. Complete **Upload** and **Mapper**, then return here.")

    c1, c2, _ = st.columns([1, 1, 6])
    with c1:
        st.button("Back", on_click=go_back)
    with c2:
        st.button("Next", on_click=go_next, type="primary")


# -----------------------------------------------------------------------------
# Page 4: Logs & Downloads
# -----------------------------------------------------------------------------
elif current == "Logs & Downloads":
    _hero("Step 4", "Logs & Downloads", "Session audit trail and CSV exports")

    col_log, col_dl = st.columns([1, 1])

    with col_log:
        st.subheader("Run log")
        if st.session_state.run_logs:
            st.json(st.session_state.run_logs)
        else:
            st.info("No events yet.")

    with col_dl:
        st.subheader("Downloads")
        if st.session_state.transformed_df is not None and not st.session_state.transformed_df.empty:
            st.download_button(
                label="⬇ Transformed dataset (CSV)",
                data=to_csv_bytes(st.session_state.transformed_df),
                file_name="dataweave_transformed.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary",
            )
        else:
            st.caption("Complete Transform to enable dataset export.")

        if st.session_state.data_dictionary:
            dd_df = pd.DataFrame(st.session_state.data_dictionary)
            st.download_button(
                label="⬇ Data dictionary (CSV)",
                data=to_csv_bytes(dd_df),
                file_name="dataweaver_data_dictionary.csv",
                mime="text/csv",
                use_container_width=True,
            )
        else:
            st.caption("Complete Mapper to enable dictionary export.")

        st.markdown("---")
        st.caption("**Engineer-generated transformation code**")
        if st.session_state.generated_code_path:
            try:
                resp = requests.get(f"{BACKEND_URL}/api/download/engineer_code", timeout=10)
                if resp.status_code == 200:
                    st.download_button(
                        label="⬇ Transformation script (.py)",
                        data=resp.content,
                        file_name="engineer_transformation.py",
                        mime="text/x-python",
                        use_container_width=True,
                    )
                else:
                    st.caption("Could not fetch code from backend.")
            except Exception:
                st.caption("Backend unreachable.")
        else:
            st.caption("Complete Transform to enable code download.")

    c1, c2, _ = st.columns([1, 1, 6])
    with c1:
        st.button("Back", on_click=go_back)
    with c2:
        st.button("Next", disabled=True)

st.markdown(
    '<p style="text-align:center;color:#94a3b8;font-size:0.8rem;margin-top:2rem;">DATA WEAVE · Capstone</p>',
    unsafe_allow_html=True,
)

