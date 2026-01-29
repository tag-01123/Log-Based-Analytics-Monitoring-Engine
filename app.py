import streamlit as st
import plotly.express as px
#import build_pipeline from backend.processing.pipeline
from backend.processing.pipeline import build_pipeline
from backend.anomaly.detector import detect_anomalies

st.title("Python Based High Throughput Log Analytics Monitoring Engine")
log_df = build_pipeline("data/sample_log.log")
anomaly_df = detect_anomalies(log_df).compute()
print(anomaly_df)

st.subheader("Anomalies Detected in Logs")

fig = px.line(anomaly_df, x='timestamp', y='anomaly_score', title='Anomaly Scores Over Time')

st.plotly_chart(fig)

st.subheader("Anomalous Log Entries")
st.dataframe(anomaly_df)


