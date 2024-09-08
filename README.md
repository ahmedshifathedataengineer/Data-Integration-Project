Scenario Recap: Data Integration from Multiple Sources
Gnosis Freight needs to integrate data from various sources like shipping partners, customs, and warehouses. The goal is to create an ETL (Extract, Transform, Load) pipeline that extracts data from multiple sources, transforms it into a unified format, and loads it into a centralized data warehouse.

Step 1: Define Data Sources and Schema
We start by identifying the data sources and their schemas. Let's assume we have three data sources:

Shipping Partner API: Provides real-time shipment data in JSON format.
Customs Data: Provided in CSV format with details of shipments cleared by customs.
Warehouse Data: Stored in a SQL database containing inventory and shipment data.
Schema Definition:

Shipping Partner API:

shipment_id (string)
origin (string)
destination (string)
status (string)
timestamp (datetime)
Customs Data (CSV):

customs_id (string)
shipment_id (string)
clearance_date (datetime)
status (string)
Warehouse Data (SQL Database):

warehouse_id (string)
shipment_id (string)
item_count (integer)
last_updated (datetime)
Step 2: Extract Data from Multiple Sources
We'll extract data from each source using Python. For simplicity, let's assume we have access to:

A REST API for shipping data.
A CSV file named customs_data.csv.
A SQL database using SQLite for warehouse data.