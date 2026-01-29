
import dask.bag as db
import os

def load_logs(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"log file not found: {file_path}")

    return db.read_text(file_path)
