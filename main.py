import time
import dask.dataframe as dd
    
from config.dask_config import start_dask
from ingestion.loader import load_logs
from ingestion.parser import parse_log_line
from processing.pipeline import build_pipeline
from config.email_config import send_email
from anomaly.detector import detect_anomaly

user_mail="gunetanmay@gmail.com"
def main():
    # Start Dask client
    client = start_dask()
    print(client)
    print(f"Dashboard link: {client.dashboard_link}")
    print("-" * 50)

    start_time = time.time()

    log_df = build_pipeline("logs/sample_log.log")

    anomalies_df = detect_anomaly(log_df)
    anomalies=anomalies_df.compute()
    if anomalies.empty:
        print("No anomalies detected.")
    else:
        print(f"{len(anomalies)} Anomalies detected:")
    
    for _, row in anomalies.iterrows():
        anomaly_data={
            "timestamp": row['timestamp'],
            "error_count": row['error_count'],
            "z_score": row['z_score']
        }
        send_email(
            to_mail=user_mail,
            anomaly=anomaly_data
        )
    print("Anomaly Detected")

    total_logs = log_df.count().compute()

    end_time = time.time()

    print("Total logs parsed:")
    print(total_logs)
    print("Time taken:", end_time - start_time)

    input("Press Enter to exit...")


if __name__ == "__main__":
    main()
