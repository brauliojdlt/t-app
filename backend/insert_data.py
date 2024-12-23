import pandas as pd
import ast
import json
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

# SQLite database setup
DATABASE_URL = "postgresql://newuser:mypassword@localhost:5432/mydatabase"
engine = create_engine(DATABASE_URL)

# Path to the CSV file
CSV_FILE = "synthetic_fraud_data.csv"
CHUNK_SIZE = 10000  # Number of rows per chunk

def parse_velocity_last_hour(value):
    """Parse the JSON-like string in the velocity_last_hour column."""
    try:
        return ast.literal_eval(value)  # Convert string to Python dict
    except Exception as e:
        print(f"Error parsing velocity_last_hour: {value}")
        return {}

def clean_and_transform_data(chunk):
    """Clean and transform a chunk of data to match the database schema."""
    # Strip and clean strings
    chunk["transaction_id"] = chunk["transaction_id"].astype(str)
    chunk["customer_id"] = chunk["customer_id"].astype(str)
    chunk["card_number"] = chunk["card_number"].astype(str)

    # Parse timestamps
    chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], utc=True, errors="coerce")

    # Drop rows with invalid timestamps
    chunk = chunk.dropna(subset=["timestamp"])

    # Convert boolean-like fields to bool
    chunk["card_present"] = chunk["card_present"].astype(bool)
    chunk["distance_from_home"] = chunk["distance_from_home"].astype(bool)
    chunk["high_risk_merchant"] = chunk["high_risk_merchant"].astype(bool)
    chunk["weekend_transaction"] = chunk["weekend_transaction"].astype(bool)
    chunk["is_fraud"] = chunk["is_fraud"].astype(bool)

    # Ensure numeric fields are the correct type
    chunk["amount"] = chunk["amount"].astype(float)
    chunk["transaction_hour"] = chunk["transaction_hour"].astype(int)

    # Parse velocity_last_hour column into dictionaries and convert to JSON strings
    chunk["velocity_last_hour"] = chunk["velocity_last_hour"].apply(
        lambda x: json.dumps(parse_velocity_last_hour(x))
    )

    return chunk

def insert_csv_data_in_chunks(csv_file, chunk_size):
    """Read a large CSV file in chunks and upload to the database."""
    total_rows = 0
    skipped_rows = 0
    
    df = pd.read_csv(csv_file)
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")
    
    try:
        # Read CSV in chunks
        
        total_rows = 0
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            chunk = clean_and_transform_data(chunk)
            chunk.to_sql("transactions", con=engine, if_exists="append", index=False, method="multi")
            total_rows += len(chunk)
            print(f"Inserted {total_rows} rows so far.")
        
        # for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
        #     # Clean and transform the chunk
        #     chunk = clean_and_transform_data(chunk)

        #     # Insert chunk into the database
        #     chunk.to_sql("transactions", con=engine, if_exists="append", index=False)
        #     total_rows += len(chunk)
        #     print(f"Inserted {len(chunk)} rows. Total: {total_rows}")

        #     print(f"Inserted {total_rows} rows. Skipped {skipped_rows} duplicates.")

        print(f"Successfully inserted {total_rows} rows with {skipped_rows} duplicates skipped.")
    
    except Exception as e:
        print("Error processing file:", str(e.__cause__))


if __name__ == "__main__":
    insert_csv_data_in_chunks(CSV_FILE, CHUNK_SIZE)
