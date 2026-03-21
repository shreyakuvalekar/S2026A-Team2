import io
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit.components.v1 as components
import streamlit as st


# Page config (must be first Streamlit command)
st.set_page_config(
    page_title="DataWeaver – Agentic Data Pipeline",
    page_icon="🧵",
    layout="wide",
    initial_sidebar_state="expanded",
)

APP_BUILD = "dataweaver-clean-v1"

# Custom styles — dashboard look & feel
def _inject_styles() -> None:
    st.markdown(
        """
        <style>
            /* Fonts & base */
            @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&display=swap');
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

    # Pre-compute tokens for cross-table FK guesses
    tokens_by_table = {name: table_tokens(name) for name in dfs.keys()}

    for name, df in dfs.items():
        primary: List[str] = []
        foreign: List[str] = []
        table_tokens_current = tokens_by_table[name]

        for col in df.columns:
            lc = col.lower().strip()
            if lc == "id":
                primary.append(col)
                continue

            # Primary key patterns: table_id, tableid, or explicit *_id that matches table token.
            if lc.endswith("_id"):
                prefix = lc[:-3]
                if prefix in table_tokens_current or lc in {f"{t}_id" for t in table_tokens_current}:
                    primary.append(col)
                else:
                    foreign.append(col)
                continue

            if any(lc == f"{t}id" for t in table_tokens_current):
                primary.append(col)
                continue

            # Foreign key patterns: references another table token with _id suffix.
            if lc.endswith("_id"):
                foreign.append(col)

        # De-duplicate while preserving order
        primary = list(dict.fromkeys(primary))
        foreign = [c for c in dict.fromkeys(foreign) if c not in primary]
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
                safe_col = re.sub(r"[^a-zA-Z0-9_]", "_", str(col))
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
        parts.append("PK: " + ", ".join(pks))
    if fks:
        parts.append("FK: " + ", ".join(fks))
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
        parts.append(f"{c} : {dt}{mark}")
    if len(cols) > max_show:
        parts.append(f"... +{len(cols) - max_show} more columns")
    text = "\\n".join(parts)
    return text.replace('"', '\\"')


def _assign_columns_to_logical_entities(
    df: pd.DataFrame, keys: Dict[str, List[str]]
) -> List[Tuple[str, List[str]]]:
    """
    Split one wide table into 4+ logical ER groups (domain attributes) — each becomes a box.
    Every column appears exactly once.
    """
    cols = list(df.columns)
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

    def core_p(c: str) -> bool:
        lc = c.lower()
        if lc in ("id", "course_id", "product_id", "order_id", "account_id"):
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

    groups: List[Tuple[str, List[str]]] = []
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
) -> str:
    pks = set(keys.get("primary_keys", []))
    fks = set(keys.get("foreign_keys", []))
    lines = [group_title, f"from: {table_file}", "────────"]
    for c in columns:
        if c in pks:
            lines.append(f"*{c}")
        elif c in fks:
            lines.append(f"+{c}  [FK]")
        else:
            lines.append(f"  {c}")
    text = "\\n".join(lines)
    return text.replace('"', '\\"')


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

    pk_txt = ", ".join(main_keys.get("primary_keys", [])[:5]) or "—"
    fk_txt = ", ".join(main_keys.get("foreign_keys", [])[:6]) or "—"

    shared_lab = (
        "Shared schema state\\n────────\\n"
        f"PK: {pk_txt}\\n"
        f"FK: {fk_txt}\\n"
        f"Inferred rels (tables): {len(rels)}"
    ).replace('"', '\\"')

    graph_title = (
        "ERD architecture — layered model (your columns distributed across logical entities)"
    ).replace('"', '\\"')

    lines = [
        "digraph ERDLayered {",
        "  compound=true;",
        '  rankdir=TB;',
        '  splines=spline;',
        '  nodesep=0.45;',
        '  ranksep=0.85;',
        f'  graph [fontname="Helvetica", fontsize=11, labelloc=t, label="{graph_title}", bgcolor="#f1f5f9", pad=0.5, fontcolor="#0f172a"];',
        '  node [fontname="Helvetica", fontsize=8];',
        '  edge [fontname="Helvetica", fontsize=7, color="#475569"];',
        "",
    ]

    # --- ① Entry (blue) — like “script / ingest”
    lines.extend(
        [
            '  subgraph cluster_entry {',
            '    label="①  Entry / ingest"; labelloc=t;',
            '    style="rounded,filled"; fillcolor="#dbeafe"; color="#2563eb"; fontcolor="#1e40af";',
            "",
        ]
    )
    for fname, df in dfs.items():
        tid = _erd_table_id(fname)
        fn_esc = fname.replace('"', '\\"')
        lines.append(
            f'    {tid} [label="SOURCE\\n{fn_esc}\\n{len(df):,} rows · {len(df.columns)} cols", '
            f'shape=note, style=filled, fillcolor="#eff6ff", color="#3b82f6", fontcolor="#1e3a8a", margin=0.12];'
        )
    lines.append("  }")
    lines.append("")

    # --- ② Logical entities (green) — 4+ boxes from real columns
    lines.extend(
        [
            '  subgraph cluster_entities {',
            '    label="②  Logical entities (attributes from your dataset)"; labelloc=t;',
            '    style="rounded,filled"; fillcolor="#dcfce7"; color="#16a34a"; fontcolor="#14532d";',
            "",
        ]
    )
    ent_ids: List[str] = []
    for i, (gtitle, gcols) in enumerate(groups):
        eid = f"ent_{i}"
        ent_ids.append(eid)
        lab = _logical_entity_box_label(gtitle, gcols, main_keys, main_name)
        lines.append(
            f'    {eid} [label="{lab}", shape=box, style="rounded,filled", fillcolor="#ffffff", '
            f'color="#15803d", penwidth=1.4, fontcolor="#14532d", margin=0.14];'
        )
    lines.append("  }")
    lines.append("")

    # --- ③ Shared keys (purple) — like etl_state
    lines.extend(
        [
            '  subgraph cluster_shared {',
            '    label="③  Shared keys & relationships"; labelloc=t;',
            '    style="rounded,filled"; fillcolor="#ede9fe"; color="#6d28d9"; fontcolor="#5b21b6";',
            f'    shared_keys [label="{shared_lab}", shape=box, style="rounded,filled", fillcolor="#ddd6fe", color="#6d28d9", penwidth=2, fontcolor="#4c1d95", margin=0.16];',
            "  }",
            "",
        ]
    )

    # --- ④ Artifacts / target (amber) — 3 boxes
    art_rows = f"{total_rows:,}"
    lines.extend(
        [
            '  subgraph cluster_artifacts {',
            '    label="④  Artifacts & target (ERD outputs)"; labelloc=t;',
            '    style="rounded,filled"; fillcolor="#fef9c7"; color="#ca8a04"; fontcolor="#854d0e";',
            f'    art_dict [label="Data dictionary\\n{total_cols} fields described", shape=box, style="rounded,filled", fillcolor="#fffbeb", color="#d97706", margin=0.1];',
            '    art_mermaid [label="Mermaid ERD text\\nexport for docs", shape=box, style="rounded,filled", fillcolor="#fffbeb", color="#d97706", margin=0.1];',
            f'    art_target [label="Profiled dataset\\n{art_rows} rows\\nready for load", shape=box, style="rounded,filled", fillcolor="#fef3c7", color="#b45309", margin=0.1];',
            "  }",
            "",
        ]
    )

    # Cross-table FK edges (physical ERD) — between real tables if any
    for parent, child, fk in rels:
        pid = _erd_table_id(parent)
        cid = _erd_table_id(child)
        fk_esc = fk.replace('"', '\\"')
        lines.append(
            f'  {pid} -> {cid} [label="{fk_esc} (FK)", color="#1d4ed8", penwidth=1.3, '
            f'constraint=false, style=bold];'
        )

    # Ingest → each logical entity (same node IDs as FK edges use)
    for fname in dfs:
        tid = _erd_table_id(fname)
        for j in range(len(ent_ids)):
            lines.append(
                f'  {tid} -> ent_{j} [label="columns", style=dashed, color="#64748b", penwidth=0.9];'
            )

    # Entities → shared (dashed, “join / keys”)
    for j, eid in enumerate(ent_ids):
        lines.append(
            f'  {eid} -> shared_keys [label="infer keys", style=dashed, color="#7c3aed", penwidth=1];'
        )

    # Shared → artifacts
    lines.extend(
        [
            '  shared_keys -> art_dict [label="feeds", color="#b45309", penwidth=1.2];',
            '  shared_keys -> art_mermaid [label="feeds", color="#b45309", penwidth=1.2];',
            '  shared_keys -> art_target [label="feeds", color="#b45309", penwidth=1.2];',
            "",
        ]
    )

    # Optional: light link between logical entities (same grain) — chain
    for j in range(len(ent_ids) - 1):
        lines.append(
            f'  ent_{j} -> ent_{j + 1} [label="1:1 via row", style=dotted, color="#86efac", '
            f'constraint=false, fontsize=7];'
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
    pk_txt = ", ".join(pk_unique[:3]) if pk_unique else "none detected yet"
    fk_txt = ", ".join(fk_unique[:3]) if fk_unique else "none detected yet"
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
        steps.append(f"Coerce dtypes for: {', '.join(cols[:8])}…")
    return steps[:4]


def render_schema_pipeline_layout_v2(
    dfs: Dict[str, pd.DataFrame],
    schema_guess: Optional[Dict[str, Dict[str, List[str]]]] = None,
) -> None:
    """Schema inference pipeline: **real column names**, keys, and join edges from your data."""
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

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Sources", len(source_names))
    with m2:
        st.metric("Relationships", len(relationships))
    with m3:
        st.metric("Fields profiled", total_cols)

    st.markdown("")

    lane_e, lane_t, lane_l = st.columns([1, 1.15, 1])

    with lane_e:
        st.markdown(
            '<p style="margin:0 0 8px 0;font-size:0.72rem;font-weight:800;letter-spacing:0.12em;color:#1d4ed8;">EXTRACT</p>',
            unsafe_allow_html=True,
        )
        src_count = min(3, len(source_names))
        for i in range(src_count):
            name = source_names[i]
            df = dfs[name]
            col_preview = ", ".join(f"`{c}`" for c in list(df.columns)[:10])
            if len(df.columns) > 10:
                col_preview += f" … **+{len(df.columns) - 10}** more"
            sg = schema_guess.get(name, {"primary_keys": [], "foreign_keys": []})
            with st.container(border=True):
                st.markdown(f"**`{name}`**")
                st.caption(f"{len(df):,} rows · {len(df.columns)} cols")
                st.markdown(f"**Columns:** {col_preview}")
                st.caption(
                    f"Per-table keys — PK: `{', '.join(sg['primary_keys']) or '—'}` · "
                    f"FK: `{', '.join(sg['foreign_keys']) or '—'}`"
                )
                q = _quality_tag(df)
                if q == "Ready":
                    st.success(q)
                elif q == "Watch":
                    st.warning(q)
                else:
                    st.error(q)
            st.markdown("")

    with lane_t:
        st.markdown(
            '<p style="margin:0 0 8px 0;font-size:0.72rem;font-weight:800;letter-spacing:0.12em;color:#b45309;">TRANSFORM</p>',
            unsafe_allow_html=True,
        )
        first_table = source_names[0]
        first_df = dfs[first_table]
        with st.container(border=True):
            st.markdown("**Inference (keys & joins)**")
            for tname in source_names[:4]:
                sg = schema_guess.get(tname, {"primary_keys": [], "foreign_keys": []})
                st.caption(
                    f"`{tname}` → PK **{', '.join(sg['primary_keys']) or '—'}** · "
                    f"FK **{', '.join(sg['foreign_keys']) or '—'}**"
                )
            if relationships:
                st.caption("**Inferred joins:**")
                for parent, child, fk in relationships[:6]:
                    st.caption(f"• `{parent}` —[{fk}]→ `{child}`")
            else:
                st.caption(
                    "**No cross-table joins inferred** — single file or no matching FK columns."
                )
            st.success("Inferred")
        with st.container(border=True):
            st.markdown(f"**Planned transforms · `{first_table}`**")
            for step in planned_transforms_named(first_df):
                st.caption(f"• {step}")
            st.warning("Awaiting run")
        with st.container(border=True):
            st.markdown("**HITL gate**")
            st.caption(
                f"Validate: `{', '.join(fk_unique[:4]) if fk_unique else 'join keys'}` "
                f"and nulls before merge."
            )
            st.warning("Needs approval")

    with lane_l:
        st.markdown(
            '<p style="margin:0 0 8px 0;font-size:0.72rem;font-weight:800;letter-spacing:0.12em;color:#15803d;">LOAD</p>',
            unsafe_allow_html=True,
        )
        with st.container(border=True):
            st.markdown("**Artifacts (this dataset)**")
            st.caption(
                f"ERD: **{len(relationships)}** edge(s) between tables "
                f"`{', '.join(source_names[:3])}` …"
            )
            st.caption(
                f"Data dictionary: **{total_cols}** fields across **{len(dfs)}** table(s)"
            )
            st.caption(f"Row count: **{total_rows:,}**")
            st.info("Export-ready")


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

def _reset_pipeline_session() -> None:
    """Clear data + mapper/ERD/schema outputs so metrics and step 2+ start fresh."""
    st.session_state.uploaded_dfs = {}
    st.session_state.mapper_output = None
    st.session_state.mapper_approved = False
    st.session_state.aggregator_output = None
    st.session_state.aggregator_report = None
    st.session_state.run_logs = []
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

    if uploaded_files:
        st.session_state.uploaded_dfs = {}
        for f in uploaded_files:
            df = pd.read_csv(f)
            st.session_state.uploaded_dfs[f.name] = df
        st.session_state.mapper_output = None
        st.session_state.mapper_approved = False
        st.session_state.aggregator_output = None
        st.session_state.aggregator_report = None
        st.session_state.run_logs = []

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
                ["Schema inference", "ERD architecture", "Data dictionary"]
            )
            with tab_schema:
                render_schema_pipeline_layout_v2(
                    st.session_state.uploaded_dfs,
                    out["schema_guess"],
                )
            with tab_erd:
                dot = generate_dataset_erd_dot(
                    st.session_state.uploaded_dfs,
                    out["schema_guess"],
                )
                st.graphviz_chart(dot, use_container_width=True)
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
        if st.button("▶ Run Aggregator (mock)", type="primary"):
            df_joined, report = run_naive_aggregator(st.session_state.uploaded_dfs)
            st.session_state.aggregator_output = df_joined
            st.session_state.aggregator_report = report
            st.session_state.run_logs.append({"stage": "Aggregator", "report": report})
            st.rerun()

        if st.session_state.aggregator_output is not None:
            rep = st.session_state.aggregator_report
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("R_final", f"{rep['R_final']:,}")
            with m2:
                st.metric("Invariant", "✓ Pass" if rep["invariant_ok"] else "✗ Fail")
            with m3:
                st.metric("Source product cap", f"{rep['R_product_sources']:,}")

            with st.expander("Full validation report", expanded=False):
                st.json(rep)

            if rep["invariant_ok"]:
                st.success("Row-count invariant satisfied.")
            else:
                st.error("Invariant violated — use **5 · HITL Checkpoint #2**.")

            st.subheader("Cleaned dataset preview")
            st.dataframe(
                st.session_state.aggregator_output.head(20),
                use_container_width=True,
                height=320,
            )

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
        if st.session_state.aggregator_output is not None:
            csv_bytes = to_csv_download(st.session_state.aggregator_output)
            st.download_button(
                label="⬇ Cleaned dataset (CSV)",
                data=csv_bytes,
                file_name="dataweaver_cleaned.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary",
            )
        else:
            st.caption("Run Aggregator to enable export.")

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
