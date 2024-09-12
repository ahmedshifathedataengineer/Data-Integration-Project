#Explanation:
#get_data_from_db() function connects to your SQLite database (freight_data.db) and retrieves all data from the integrated_shipment_data table.
#he /api/getData route returns the data as a JSON response.

# app.py
import pandas as pd
from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

def get_data_from_db():
    try:
        conn = sqlite3.connect('freight_data.db')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM integrated_shipment_data')
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []

def check_data_quality(df):
    """
    Checks for missing values and prints a summary of data quality issues.
    """
    missing_data = df.isnull().sum()
    print("Missing Data Summary:")
    print(missing_data[missing_data > 0])

@app.route('/')
def home():
    return "Welcome to the Freight Data Dashboard API!"

@app.route('/api/getData', methods=['GET'])
def get_data():
    data = get_data_from_db()
    data_df = pd.DataFrame(data)
    check_data_quality(data_df)  # Perform data quality checks
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
