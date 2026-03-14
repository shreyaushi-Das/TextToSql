from dotenv import load_dotenv
load_dotenv()

import streamlit as st
import os
import sqlite3
import google.generativeai as genai
from csv_to_sqlite import process_file

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))


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
    model = genai.GenerativeModel('gemini-2.0-flash')
    response = model.generate_content(prompt)
    return response.text.strip()


def run_sql_query(sql, db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    col_names = [description[0] for description in cur.description]
    conn.close()
    return rows, col_names


# ── Streamlit UI ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Text to SQL")
st.header("Conversational BI — Ask Your Data Anything")

# Step 1: File upload
uploaded_file = st.file_uploader("Upload your CSV or Excel file", type=["csv", "xlsx", "xls"])

if uploaded_file is not None:
    # Process the file and store schema in session
    if "schema_string" not in st.session_state or st.session_state.get("filename") != uploaded_file.name:
        with st.spinner("Reading file and building database..."):
            try:
                uploaded_file.seek(0)
                result = process_file(uploaded_file, db_path="database.db")
                st.session_state["schema_string"] = result["schema_string"]
                st.session_state["db_path"] = result["db_path"]
                st.session_state["filename"] = uploaded_file.name
                st.session_state["columns"] = result["original_columns"]
                st.session_state["row_count"] = result["row_count"]
            except Exception as e:
                st.error(f"Could not process file: {e}")
                st.stop()

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
                st.error(f"Something went wrong: {e}")
