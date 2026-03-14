from dotenv import load_dotenv
load_dotenv()

import streamlit as st
import os
import sqlite3
import hashlib
import traceback
from google import genai
from csv_to_sqlite import process_file

# ── Cache the client so it's created ONCE, not on every rerender ──────────────
@st.cache_resource
def get_client():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        st.error("❌ GOOGLE_API_KEY not found in environment. Check your .env file.")
        st.stop()
    return genai.Client(api_key=api_key)

client = get_client()


def get_gemini_response(question, schema_string):
    prompt = f"""
    You are an expert in converting English questions to SQL queries!
    
    The database has the following structure:
    {schema_string}
    
    Convert the user's question to a valid SQLite query.
    Rules:
    - Return ONLY the SQL query, nothing else
    - No backticks, no "sql" word, no explanation
    - Use exact column names from the schema above
    
    Question: {question}
    """
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[prompt]
    )
    return response.text.strip()


def run_sql_query(sql, db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    col_names = [description[0] for description in cur.description]
    conn.close()
    return rows, col_names


def file_hash(uploaded_file):
    """Hash file contents so we only reprocess when the file actually changes."""
    uploaded_file.seek(0)
    h = hashlib.md5(uploaded_file.read()).hexdigest()
    uploaded_file.seek(0)
    return h


# ── Streamlit UI ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Text to SQL")
st.header("Conversational BI — Ask Your Data Anything")

# Step 1: File upload
uploaded_file = st.file_uploader("Upload your CSV or Excel file", type=["csv", "xlsx", "xls"])

if uploaded_file is not None:
    # Use file hash to detect actual file changes — not just filename
    current_hash = file_hash(uploaded_file)

    if st.session_state.get("file_hash") != current_hash:
        with st.spinner("Reading file and building database..."):
            # Show full error — don't silently swallow it
            result = process_file(uploaded_file, db_path="database.db")
            st.session_state["schema_string"] = result["schema_string"]
            st.session_state["db_path"] = result["db_path"]
            st.session_state["filename"] = uploaded_file.name
            st.session_state["columns"] = result["original_columns"]
            st.session_state["row_count"] = result["row_count"]
            st.session_state["file_hash"] = current_hash

    # Show file info
    st.success(f"✅ Loaded **{uploaded_file.name}** — {st.session_state['row_count']} rows, {len(st.session_state['columns'])} columns")
    with st.expander("See columns"):
        st.write(st.session_state["columns"])

    # Step 2: Ask question
    st.subheader("Ask a question about your data")
    question = st.text_input("e.g. Show me total revenue by region", key="input")
    submit = st.button("Ask")

    if submit and question:
        with st.spinner("Generating SQL and fetching results..."):
            try:
                # Get SQL from Gemini
                sql = get_gemini_response(question, st.session_state["schema_string"])
                st.code(sql, language="sql")

                # Run SQL on database
                rows, col_names = run_sql_query(sql, st.session_state["db_path"])

                # Show results
                st.subheader("Results")
                if rows:
                    import pandas as pd
                    df_result = pd.DataFrame(rows, columns=col_names)
                    st.dataframe(df_result)
                else:
                    st.info("Query returned no results.")

            except Exception as e:
                # Show the FULL error — this is what was hiding your real problem
                st.error(f"❌ Error: {e}")
                st.code(traceback.format_exc(), language="python")