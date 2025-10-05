
import os
import glob
import time
import duckdb
import pandas as pd
import streamlit as st

LAKE_DIR = os.path.join("lake", "watch_agg")

st.set_page_config(page_title="StreamHub Analytics", layout="wide")
st.title("📊 StreamHub Analytics — Watch Aggregations (per minute)")

pattern = os.path.join(LAKE_DIR, "dt=*/part-*.parquet")
files = glob.glob(pattern)
if not files:
    st.info("No Parquet data yet. Start the ingest server and streaming job, then generate load.")
    st.stop()

con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

result = con.execute(f"""
    SELECT
      CAST(window_start AS TIMESTAMP) AS window_start,
      user_id,
      total_watch_seconds
    FROM read_parquet('{pattern}')
""").fetchdf()

left, right = st.columns(2)

with left:
    st.subheader("Top Users by Watch Time")
    topn = st.slider("Top N", 5, 50, 10, 5)
    top_users = (result.groupby("user_id", as_index=False)["total_watch_seconds"]
                        .sum().sort_values("total_watch_seconds", ascending=False).head(topn))
    st.dataframe(top_users)

with right:
    st.subheader("Watch Time Over Time (Total)")
    by_time = (result.groupby("window_start", as_index=False)["total_watch_seconds"].sum()
                     .sort_values("window_start"))
    st.line_chart(by_time.set_index("window_start"))

st.caption("Data partitions: " + str(len(files)) + " files")
