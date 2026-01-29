import dask.dataframe as dd
import dask.bag as db

from ingestion.loader import load_logs
from ingestion.parser import parse_log_line


def build_pipeline(file_path):
    """
    Builds a Dask-based log processing pipeline
    """

    # Load log file as Dask Bag
    bag = load_logs(file_path)

    # Parse log lines and remove invalid entries
    parsed_bag = (
        bag
        .map(parse_log_line)
        .filter(lambda x: x is not None)
    )

    # Metadata for DataFrame
    meta = {
        "timestamp": "object",
        "level": "object",
        "service": "object",
        "message": "object"
    }
    
    # Convert Bag to DataFrame
    df = parsed_bag.to_dataframe(meta=meta)

    # Convert timestamp column to datetime
    df["timestamp"] = dd.to_datetime(df["timestamp"],
                                     errors="coerce"
                                     )
    
    return df
