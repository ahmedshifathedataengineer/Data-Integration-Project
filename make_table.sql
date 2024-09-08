CREATE TABLE IF NOT EXISTS warehouse_data (
    shipment_id INTEGER PRIMARY KEY,
    product_name TEXT,
    quantity INTEGER,
    last_updated DATE
);

INSERT INTO warehouse_data (shipment_id, product_name, quantity, last_updated)
VALUES (1, 'Product A', 100, '2024-09-07'),
       (2, 'Product B', 200, '2024-09-08');
