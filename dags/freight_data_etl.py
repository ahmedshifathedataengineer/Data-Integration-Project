from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['your-email@example.com'],
    'email_on_failure': True,
}

# Define the DAG
with DAG('freight_data_etl', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    # Task 1: Check if Shipping Partner API is available
    check_api_availability = HttpSensor(
        task_id='check_api_availability',
        http_conn_id='shipping_partner_api',
        endpoint='iss-now',
        timeout=20,
        retries=3
    )

    # Task 2: Extract Data from API
    extract_api_data = PythonOperator(
        task_id='extract_api_data',
        python_callable=your_function_to_extract_api_data
    )

    # Task 3: Check if CSV File is Available
    check_csv_file = BashOperator(
        task_id='check_csv_file',
        bash_command='test -f /path/to/import_export_data.csv'
    )

    # Task 4: Extract Data from CSV
    extract_csv_data = PythonOperator(
        task_id='extract_csv_data',
        python_callable=your_function_to_extract_csv_data
    )

    # Task 5: Extract Data from SQLite Database
    extract_sqlite_data = SqliteOperator(
        task_id='extract_sqlite_data',
        sqlite_conn_id='sqlite_default',
        sql='SELECT * FROM your_table_name'
    )

    # Task 6: Transform Data
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=your_function_to_transform_data
    )

    # Task 7: Load Data into SQLite Database
    load_data = SqliteOperator(
        task_id='load_data',
        sqlite_conn_id='sqlite_default',
        sql='INSERT INTO your_table_name VALUES(...)'
    )

    # Task 8: Send Error Notification
    send_error_email = EmailOperator(
        task_id='send_error_email',
        to='your-email@example.com',
        subject='ETL Pipeline Failed',
        html_content='<p>The ETL pipeline encountered an error. Please check the logs for details.</p>'
    )

    # Define Task Dependencies
    check_api_availability >> extract_api_data >> [check_csv_file, extract_sqlite_data] >> extract_csv_data >> transform_data >> load_data
