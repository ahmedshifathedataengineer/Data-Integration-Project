# This is the main ETL pipeline script, which integrates all components: extraction, transformation, and loading.
# etl_pipeline.py

import pandas as pd
from shipping_data import extract_shipping_data
import sqlite3

# Paths to data files
# Corrected paths using raw strings
csv_path = r'C:\Users\hurri\OneDrive\Desktop\Freight Data Integration\import_export_data.csv'
db_path = r'C:\Users\hurri\OneDrive\Desktop\Freight Data Integration\freight_data.db'
api_url = "http://api.open-notify.org/iss-now.json"
table_name = 'integrated_shipment_data'

# Extract Customs Data
def extract_customs_data(csv_path):
    """
    Extracts data from a CSV file representing customs data.
    """
    try:
        customs_df = pd.read_csv(csv_path)
        return customs_df
    except FileNotFoundError:
        print(f"File not found: {csv_path}")
        return pd.DataFrame()
    except pd.errors.EmptyDataError:
        print("CSV file is empty")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return pd.DataFrame()

# Extract Warehouse Data
def extract_warehouse_data(db_path):
    """
    Extracts data from an SQLite database representing warehouse data.
    """
    try:
        conn = sqlite3.connect(db_path)
        query = "SELECT * FROM warehouse_data"
        warehouse_df = pd.read_sql_query(query, conn)
        conn.close()
        return warehouse_df
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred while extracting data from the database: {e}")
        return pd.DataFrame()

# Transform Data
def transform_data(shipping_df, customs_df, warehouse_df):
    """
    Transforms the extracted data by standardizing date formats and merging the datasets.
    """
    # Check if any dataframe is empty
    if shipping_df.empty or customs_df.empty or warehouse_df.empty:
        print("One or more data sources are empty. Skipping transformation.")
        return pd.DataFrame()

    try:
        # Standardizing date formats
        shipping_df['timestamp'] = pd.to_datetime(shipping_df['timestamp'], errors='coerce')
        customs_df['Date'] = pd.to_datetime(customs_df['Date'], errors='coerce')
        warehouse_df['last_updated'] = pd.to_datetime(warehouse_df['last_updated'], errors='coerce')

        # Create a shipment_id column if it doesn't exist
        if 'shipment_id' not in shipping_df.columns:
            shipping_df['shipment_id'] = range(1, len(shipping_df) + 1)
        if 'shipment_id' not in warehouse_df.columns:
            warehouse_df['shipment_id'] = range(1, len(warehouse_df) + 1)

        # Merging dataframes
        merged_df = pd.merge(shipping_df, customs_df, left_on='timestamp', right_on='Date', how='left')
        merged_df = pd.merge(merged_df, warehouse_df, on='shipment_id', how='left')

        # Convert datetime columns to string format to handle SQLite limitations
        for col in merged_df.select_dtypes(include=['datetime64[ns]']).columns:
            merged_df[col] = merged_df[col].astype(str)

        merged_df.fillna('N/A', inplace=True)
        print("Data Transformation Successful")
        return merged_df

    except Exception as e:
        print(f"An error occurred during data transformation: {e}")
        return pd.DataFrame()

# Load Data
def load_data_to_sqlite(transformed_df, db_path, table_name):
    """
    Loads transformed data into an SQLite database.
    """
    if transformed_df.empty:
        print("Transformed data is empty. Skipping data load.")
        return

    try:
        # Using a context manager to handle the SQLite connection
        with sqlite3.connect(db_path) as conn:
            print(f"Connected to database at {db_path}")
            transformed_df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"Data successfully loaded to {table_name} in SQLite.")
    except Exception as e:
        print(f"An error occurred while loading data to SQLite: {e}")

# ETL Pipeline
def etl_pipeline():
    # Extract data from sources
    shipping_df = extract_shipping_data(api_url)
    customs_df = extract_customs_data(csv_path)
    warehouse_df = extract_warehouse_data(db_path)

    # Transform the extracted data
    transformed_df = transform_data(shipping_df, customs_df, warehouse_df)

    # Right before loading, add a debug print
    if transformed_df.empty:
        print("Transformed data is empty. Skipping data load.")
    else:
        print("Transformed Data:")
        print(transformed_df.head())

        # Load transformed data into the database
        load_data_to_sqlite(transformed_df, db_path, table_name)

# Run the ETL pipeline
if __name__ == "__main__":
    etl_pipeline()
