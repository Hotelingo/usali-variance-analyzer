#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from usali_pipeline import ingest

DB_PATH = Path("usali.db")

st.set_page_config(page_title="USALI Variance Analyzer", layout="wide")
st.title("USALI Variance Analyzer")
st.caption("Upload a monthly USALI workbook, run ingestion, and review extracted outputs before analysis.")

with st.form("upload_form"):
    upload_month = st.text_input("Upload month (required)", placeholder="YYYY-MM")
    uploaded_file = st.file_uploader("USALI workbook (.xlsx)", type=["xlsx"])
    submitted = st.form_submit_button("Run pipeline and update database")

if submitted:
    if not upload_month.strip():
        st.error("Please provide the upload month.")
    elif uploaded_file is None:
        st.error("Please upload a workbook file.")
    else:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file.getbuffer())
            tmp_path = Path(tmp.name)

        try:
            import_id = ingest(tmp_path, DB_PATH, upload_month=upload_month.strip())
            st.success(f"Pipeline completed. import_id={import_id} saved to {DB_PATH}")
            st.session_state["latest_import_id"] = import_id
        finally:
            tmp_path.unlink(missing_ok=True)

latest_import_id = st.session_state.get("latest_import_id")

if DB_PATH.exists():
    conn = sqlite3.connect(DB_PATH)
    imports_df = pd.read_sql_query(
        """
        SELECT id, file_name, period, uploaded_month, bu_code, budget_ledger, forecast_version, imported_at
        FROM imports
        ORDER BY id DESC
        LIMIT 12
        """,
        conn,
    )
    if imports_df.empty:
        st.info("No data ingested yet.")
    else:
        if latest_import_id is None:
            latest_import_id = int(imports_df.iloc[0]["id"])

        st.subheader("Recent uploads")
        st.dataframe(imports_df, width="stretch")

        col1, col2, col3 = st.columns(3)
        sl_count = conn.execute(
            "SELECT COUNT(*) FROM statement_lines WHERE import_id=?", (latest_import_id,)
        ).fetchone()[0]
        af_count = conn.execute(
            "SELECT COUNT(*) FROM account_facts WHERE import_id=?", (latest_import_id,)
        ).fetchone()[0]
        coa_count = conn.execute("SELECT COUNT(*) FROM chart_of_accounts").fetchone()[0]
        col1.metric("Statement lines", f"{sl_count:,}")
        col2.metric("Account facts", f"{af_count:,}")
        col3.metric("Chart of accounts", f"{coa_count:,}")

        st.subheader("Preview: Chart of Accounts")
        coa_df = pd.read_sql_query(
            "SELECT account_code, first_seen_import, last_seen_import FROM chart_of_accounts ORDER BY account_code LIMIT 200",
            conn,
        )
        st.dataframe(coa_df, width="stretch")

        st.subheader("Preview: Cost Centers")
        cc_df = pd.read_sql_query(
            "SELECT cost_center_code, first_seen_import, last_seen_import FROM cost_centers ORDER BY cost_center_code LIMIT 200",
            conn,
        )
        st.dataframe(cc_df, width="stretch")

        st.subheader(f"Preview: Statement lines (import_id={latest_import_id})")
        lines_df = pd.read_sql_query(
            """
            SELECT sheet_name, section, row_no, line_label, actual, budget, prior_year,
                   variance_budget, variance_prior_year
            FROM statement_lines
            WHERE import_id=?
            ORDER BY sheet_name, row_no
            LIMIT 300
            """,
            conn,
            params=(latest_import_id,),
        )
        st.dataframe(lines_df, width="stretch")

        st.subheader(f"Preview: Account facts (import_id={latest_import_id})")
        facts_df = pd.read_sql_query(
            """
            SELECT block_index, scenario_period, scenario_ledger, account_code,
                   cost_center_code, market_segment_code, amount
            FROM account_facts
            WHERE import_id=?
            ORDER BY block_index, account_code, cost_center_code
            LIMIT 300
            """,
            conn,
            params=(latest_import_id,),
        )
        st.dataframe(facts_df, width="stretch")

        st.info("If the previews look correct, you can proceed to trend/variance analysis in the next step.")

    conn.close()
else:
    st.info("No database found yet. Upload a workbook to initialize usali.db.")
