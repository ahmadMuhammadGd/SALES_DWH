DROP TABLE IF EXISTS CSV_STAGING;


CREATE TABLE IF NOT EXISTS CSV_STAGING(
invoice_id TEXT,
branch_name TEXT,
city TEXT,
client_fname TEXT,
client_lname TEXT,
salesman_fname TEXT,
salesman_lname TEXT,
client_email TEXT,
client_phone TEXT,
product_name TEXT,
product_line TEXT,
product_price TEXT,
amount TEXT,
order_date TEXT,
order_time TEXT,
payment_method TEXT
);


LOAD DATA INFILE "/SQL-files/cleaned_data.csv"
INTO TABLE CSV_STAGING
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- TEST DUMMY DATA
-- INSERT INTO CSV_STAGING VALUES
-- (1, 'Central', 'New York', 'John', 'Doe', 'Alice', 'Smith', 'john.doe@example.com', '555-1234', 'Laptop', 'Electronics', '1000', '1', '2024-06-01', '10:00:00', 'Credit Card'),
-- (2, 'East', 'Boston', 'Jane', 'Doe', 'Bob', 'Johnson', 'jane.doe@example.com', '555-5678', 'Smartphone', 'Electronics', '800', '2', '2024-06-02', '11:30:00', 'Cash'),
-- (3, 'West', 'San Francisco', 'Emily', 'Clark', 'Charlie', 'Brown', 'emily.clark@example.com', '555-8765', 'Tablet', 'Electronics', '600', '3', '2024-06-03', '14:15:00', 'Debit Card'),
-- (4, 'North', 'Chicago', 'Michael', 'Miller', 'David', 'Davis', 'michael.miller@example.com', '555-4321', 'Headphones', 'Accessories', '150', '4', '2024-06-04', '16:45:00', 'Credit Card'),
-- (5, 'South', 'Miami', 'Sarah', 'Wilson', 'Edward', 'Martinez', 'sarah.wilson@example.com', '555-6789', 'Camera', 'Photography', '1200', '5', '2024-06-05', '09:00:00', 'Cash');
-- (6, 'Central', 'New York', 'William', 'Johnson', 'Alice', 'Smith', 'william.johnson@example.com', '555-7890', 'Printer', 'Office Supplies', '200', '1', '2024-06-06', '13:30:00', 'Debit Card'),
-- (6, 'Central', 'New York', 'William', 'Johnson', 'Alice', 'Smith', 'william.johnson@example.com', '555-7890', 'Printer-5', 'Office Supplies', '2000', '12', '2024-06-06', '13:30:00', 'Debit Card'),
-- (7, 'East', 'Boston', 'Olivia', 'Martinez', 'Bob', 'Johnson', 'olivia.martinez@example.com', '555-8901', 'Monitor', 'Electronics', '300', '2', '2024-06-07', '15:00:00', 'Credit Card'),
-- (8, 'West', 'San Francisco', 'James', 'Brown', 'Charlie', 'Brown', 'james.brown@example.com', '555-9012', 'Keyboard', 'Accessories', '50', '3', '2024-06-08', '17:30:00', 'Cash'),
-- (9, 'North', 'Chicago', 'Ava', 'Davis', 'David', 'Davis', 'ava.davis@example.com', '555-0123', 'Mouse', 'Accessories', '25', '4', '2024-06-09', '08:45:00', 'Debit Card'),
-- (71, 'East', 'Boston', 'Olivia', 'Martinez', 'Bob', 'Johnson', 'olivia.martinez@example.com', '555-8901', 'Monitor', 'Electronics', '360', '2', '2024-06-10', '15:00:00', 'Credit Card')
-- ;