# What is problem we need slove?
# - > Services, or, Application or servers may generate a large no of log data every day
# - > If sometimes errors increase suddenly, this sudden increase are called anamoly
# - > Detect anamoly and send alert using webhooks(URL)

# What Is Z_Score?
# Z_score tells us how far a value is value is from normal behaviour

# Formaula

# z = x - u / sigma

# What is data present in log data

# 1. timestamp
# 2. Level
# 3. Service
# 4. Message


# Logs data -----> Takes only errors from log data -----> Count the errors per minute(based on log data)------->Find normal behaviour (mean) ----------> cal z scrore--------> flag anamoly ---->alert customer/organization


# Import dask.dataframe as dd

# Step 1: define function for anamoly delect
# step 2: Count errors per minute based log data
# step 3: Find the Normal behaviour
# step 5: calculate z score
# step 6 : Delect anamoly
# step 7 : return anamoly
import dask.dataframe as dd
import os
"""
    Detects anomalies in log data based on Z-score method.
    
    Parameters:
    - log_df: Dask DataFrame with log data containing 'timestamp' and 'level' columns.
    - z_threshold: Z-score threshold to flag anomalies.
    
    Returns:
    - Dask DataFrame with anomalies flagged.
    """
   
def detect_anomaly(log_df, z_threshold=1):
    log_df['timestamp'] = dd.to_datetime(log_df['timestamp'] )
    log_df['level'] = log_df["level"].str.strip().str.upper()

    error_logs = log_df[log_df['level'] == 'ERROR']
    error_logs.compute()

    # Create minute bucket
    error_logs["minute"] = error_logs["timestamp"].dt.floor("min")

    # Count errors per minute
    error_counts = (
        error_logs
        .groupby("minute")
        .size()
        .rename("error_count")
        .reset_index()
    ).compute()

    # Compute statistics
    mean = error_counts["error_count"].mean()
    std  = error_counts["error_count"].std()

    if std == 0:
        error_counts["z_score"] = 0
        error_counts["is_anomaly"] = True
        return error_counts

    error_counts["z_score"] = (error_counts["error_count"] - mean) / std
    error_counts["is_anomaly"] = error_counts["z_score"].abs() > z_threshold

    return error_counts[error_counts["is_anomaly"]]
