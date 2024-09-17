# Freight Data Integration

The Freight Data Integration project is an ETL (Extract, Transform, Load) pipeline that integrates data from multiple sources, including shipping partner APIs, customs data in CSV format, and warehouse data stored in a SQLite database. The goal is to merge and transform this data to provide comprehensive insights into freight operations.

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Data Sources](#data-sources)
- [Setup Instructions](#setup-instructions)
- [Running the ETL Pipeline](#running-the-etl-pipeline)
- [Project Structure](#project-structure)
- [Error Handling](#error-handling)
- [Screenshots](#screenshots)
- [Contributing](#contributing)
- [License](#license)

## Project Overview

This project aims to build a robust ETL pipeline that integrates various freight-related datasets into a single source of truth. It extracts data from:

- **Shipping Partner API:** Simulated data using a publicly available API.
- **Customs Data:** A CSV file containing import and export data.
- **Warehouse Data:** Data stored in a SQLite database.

The pipeline transforms this data by standardizing formats and merging datasets based on a common key (`shipment_id`). The final transformed data is stored in a SQLite database.

## Features

- **Data Extraction:** Extracts data from an API, CSV file, and SQLite database.
- **Data Transformation:** Cleans and merges data to ensure consistency.
- **Data Loading:** Loads the transformed data into a SQLite database.
- **Error Handling:** Robust error handling to manage missing files, empty data, and failed connections.

## Technologies Used

- **Python:** Core programming language used for developing the ETL pipeline.
- **SQLite:** Lightweight database used to store and manage the warehouse data.
- **Pandas:** Python library used for data manipulation and transformation.
- **Requests:** Python library used for making HTTP requests to APIs.

## Data Sources

- **Shipping Partner API:**
  - **API Endpoint:** [Publicly Available API - ISS Current Location](http://api.open-notify.org/iss-now.json)
  - **Description:** Simulates real-time shipping data.

- **Customs Data (CSV File):**
  - **File Name:** `import_export_data.csv`
  - **Description:** A CSV file containing simulated import and export data.

- **Warehouse Data (SQLite Database):**
  - **File Name:** `freight_data.db`
  - **Description:** Stores information on warehouse inventory, shipment details, etc.

## Setup Instructions

### Prerequisites

- Python 3.8 or higher
- SQLite installed (if not using a SQLite extension in VS Code)
- Required Python packages: `pandas`, `requests`

### Installation

1. **Clone the Repository:**

    ```bash
    git clone https://github.com/yourusername/freight-data-integration.git
    cd freight-data-integration
    ```

2. **Install the Required Packages:**

    ```bash
    pip install pandas requests
    ```

## Running the ETL Pipeline

1. **Extract data from sources:**
   - Run the `shipping_data.py` script to extract data from the Shipping Partner API.

    ```bash
    python shipping_data.py
    ```

2. **Execute the ETL pipeline:**
   - Run the `etl_pipeline.py` script to extract, transform, and load data from all sources into the SQLite database.

    ```bash
    python etl_pipeline.py
    ```

## Project Structure

```plaintext
freight_data_integration/
├── etl_pipeline.py               # Main ETL pipeline script
├── shipping_data.py              # Extracts data from the Shipping Partner API
├── import_export_data.csv        # Simulated customs data file
├── freight_data.db               # SQLite database for warehouse data
├── README.md                     # Project documentation

