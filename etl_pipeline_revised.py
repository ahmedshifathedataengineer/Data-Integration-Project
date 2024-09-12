# This script serves as the main ETL (Extract, Transform, Load) pipeline, integrating extraction, transformation, and loading processes for freight data.
# etl_pipeline.py

import pandas as pd  # Import the pandas library for data manipulation
from shipping_data import extract_shipping_data  # Import a custom function to extract shipping data from an API
import sqlite3  # Import SQLite library to interact with the SQLite database

# Define paths and constants for data files
csv_path = r'C:\Users\hurri\OneDrive\Desktop\Freight Data Integration\import_export_data.csv'  # Path to customs data CSV file
db_path = r'C:\Users\hurri\OneDrive\Desktop\Freight Data Integration\freight_data.db'  # Path to the SQLite database file
api_url = "http://api.open-notify.org/iss-now.json"  # API endpoint URL for shipping data (simulated with ISS API)
table_name = 'integrated_shipment_data'  # Name of the table where the integrated data will be stored

# Extract customs data from a CSV file
def extract_customs_data(csv_path):
    """
    Extracts data from a CSV file representing customs data.
    """
    try:
        customs_df = pd.read_csv(csv_path)  # Read the CSV file into a pandas DataFrame
        return customs_df
    except FileNotFoundError:
        print(f"File not found: {csv_path}")  # Handle missing file error
        return pd.DataFrame()  # Return an empty DataFrame to avoid breaking the pipeline
    except pd.errors.EmptyDataError:
        print("CSV file is empty")  # Handle empty CSV file error
        return pd.DataFrame()  # Return an empty DataFrame for empty file
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")  # Handle any other errors
        return pd.DataFrame()  # Return an empty DataFrame on any exception

# Extract warehouse data from an SQLite database
def extract_warehouse_data(db_path):
    """
    Extracts data from an SQLite database representing warehouse data.
    """
    try:
        conn = sqlite3.connect(db_path)  # Establish connection to SQLite database
        query = "SELECT * FROM warehouse_data"  # SQL query to retrieve all records from the warehouse table
        warehouse_df = pd.read_sql_query(query, conn)  # Execute the query and load data into a DataFrame
        conn.close()  # Close the database connection after use
        return warehouse_df
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")  # Handle SQLite-specific errors
        return pd.DataFrame()  # Return an empty DataFrame on SQLite error
    except Exception as e:
        print(f"An error occurred while extracting data from the database: {e}")  # Handle other exceptions
        return pd.DataFrame()  # Return an empty DataFrame on any exception

# Transform the extracted data
def transform_data(shipping_df, customs_df, warehouse_df):
    """
    Transforms the extracted data by standardizing date formats and merging the datasets.
    """
    # Check if any input DataFrame is empty
    if shipping_df.empty or customs_df.empty or warehouse_df.empty:
        print("One or more data sources are empty. Skipping transformation.")  # Skip transformation if data is missing
        return pd.DataFrame()  # Return an empty DataFrame if any source is empty

    try:
        # Standardize date formats to ensure consistency across datasets
        shipping_df['timestamp'] = pd.to_datetime(shipping_df['timestamp'], errors='coerce')
        customs_df['Date'] = pd.to_datetime(customs_df['Date'], errors='coerce')
        warehouse_df['last_updated'] = pd.to_datetime(warehouse_df['last_updated'], errors='coerce')

        # Create a 'shipment_id' column if it doesn't exist in the DataFrames
        if 'shipment_id' not in shipping_df.columns:
            shipping_df['shipment_id'] = range(1, len(shipping_df) + 1)
        if 'shipment_id' not in warehouse_df.columns:
            warehouse_df['shipment_id'] = range(1, len(warehouse_df) + 1)

        # Merge dataframes on common keys (timestamp and shipment_id)
        merged_df = pd.merge(shipping_df, customs_df, left_on='timestamp', right_on='Date', how='left')
        merged_df = pd.merge(merged_df, warehouse_df, on='shipment_id', how='left')

        # Convert datetime columns to string format due to SQLite limitations
        for col in merged_df.select_dtypes(include=['datetime64[ns]']).columns:
            merged_df[col] = merged_df[col].astype(str)

        merged_df.fillna('N/A', inplace=True)  # Fill missing values with 'N/A'
        print("Data Transformation Successful")  # Log successful transformation
        return merged_df  # Return the transformed DataFrame

    except Exception as e:
        print(f"An error occurred during data transformation: {e}")  # Handle transformation errors
        return pd.DataFrame()  # Return an empty DataFrame on any exception

# Load transformed data into the SQLite database
def load_data_to_sqlite(transformed_df, db_path, table_name):
    """
    Loads transformed data into an SQLite database.
    """
    if transformed_df.empty:
        print("Transformed data is empty. Skipping data load.")  # Skip loading if no data to load
        return

    try:
        # Use a context manager to handle the SQLite connection safely
        with sqlite3.connect(db_path) as conn:
            print(f"Connected to database at {db_path}")  # Confirm connection to the database
            transformed_df.to_sql(table_name, conn, if_exists='replace', index=False)  # Insert data into SQLite table
            print(f"Data successfully loaded to {table_name} in SQLite.")  # Log success message
    except Exception as e:
        print(f"An error occurred while loading data to SQLite: {e}")  # Handle loading errors

# Main ETL pipeline function that orchestrates the extraction, transformation, and loading processes
def etl_pipeline():
    # Step 1: Extract data from all sources
    shipping_df = extract_shipping_data(api_url)
    customs_df = extract_customs_data(csv_path)
    warehouse_df = extract_warehouse_data(db_path)

    # Step 2: Transform the extracted data into a unified format
    transformed_df = transform_data(shipping_df, customs_df, warehouse_df)

    # Debug print to display a preview of the transformed data
    if transformed_df.empty:
        print("Transformed data is empty. Skipping data load.")  # Skip loading if data is empty
    else:
        print("Transformed Data Preview:")  # Print preview of transformed data
        print(transformed_df.head())  # Display the first few rows of the transformed DataFrame

        # Step 3: Load the transformed data into the SQLite database
        load_data_to_sqlite(transformed_df, db_path, table_name)

# Entry point to run the ETL pipeline
if __name__ == "__main__":
    etl_pipeline()  # Execute the ETL pipeline function
