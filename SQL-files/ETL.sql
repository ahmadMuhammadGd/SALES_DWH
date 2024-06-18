USE DWH;
--populating ETL_BATCH
SET
  @CURRENT_BATCH_ID = (
    SELECT
      COALESCE(MAX(batch_id), 0) AS next_id
    FROM
      ETL_BATCH
    WHERE (start_time IS NOT NULL) AND (finish_time IS NOT NULL)
  ) + 1;

INSERT INTO ETL_BATCH (batch_id, start_time)
VALUES (@CURRENT_BATCH_ID, NOW()); 

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

-- EXTRACT BRANCHES
INSERT INTO BRANCHES(branch_name, city)
SELECT DISTINCT branch_name, city
FROM CSV_STAGING
WHERE (branch_name, city) NOT IN (
    SELECT branch_name, city FROM BRANCHES
    );

-- Extract unique clients
INSERT INTO CLIENTS (first_name, last_name)
SELECT DISTINCT client_fname, client_lname
FROM CSV_STAGING
WHERE (client_fname, client_lname) NOT IN (
    SELECT first_name, last_name
    FROM CLIENTS
) 
AND client_phone NOT IN (SELECT phone_number FROM CLIENT_PHONES)
AND client_email NOT IN (SELECT email FROM CLIENT_EMAILS);

--EXTRACT CLIENT EMAILS 
INSERT INTO CLIENT_EMAILS (person_id, email)
SELECT DISTINCT CLIENTS.client_id, CSV_STAGING.client_email
FROM CSV_STAGING
LEFT JOIN CLIENTS ON CLIENTS.first_name = CSV_STAGING.client_fname 
AND CLIENTS.last_name = CSV_STAGING.client_lname
WHERE CSV_STAGING.client_email IS NOT NULL
AND CSV_STAGING.client_email NOT IN (
    SELECT email FROM CLIENT_EMAILS
);

-- Extract client phones
INSERT INTO CLIENT_PHONES (person_id, phone_number)
SELECT DISTINCT CLIENTS.client_id, CSV_STAGING.client_phone
FROM CSV_STAGING
JOIN CLIENTS ON CLIENTS.first_name = CSV_STAGING.client_fname 
AND CLIENTS.last_name = CSV_STAGING.client_lname
WHERE CSV_STAGING.client_phone IS NOT NULL
AND CSV_STAGING.client_phone NOT IN (
    SELECT phone_number FROM CLIENT_PHONES
);


-- Extract salesmen
INSERT INTO SALESMEN (first_name, last_name)
SELECT DISTINCT salesman_fname, salesman_lname
FROM CSV_STAGING
WHERE CSV_STAGING.salesman_fname NOT IN (SELECT first_name FROM SALESMEN)
AND CSV_STAGING.salesman_lname NOT IN (SELECT last_name FROM SALESMEN);


-- Extract products
INSERT INTO PRODUCTS (product_name, product_line)
SELECT DISTINCT product_name, product_line
FROM CSV_STAGING
WHERE (product_name, product_line) NOT IN (SELECT product_name, product_line FROM PRODUCTS);


-- EXTRACT PRICES
-- PRICES is a slowly changing dimension type 2
-- STEP 1: CLOSE OLD PRICES
UPDATE PRICES
INNER JOIN (
  SELECT P.product_id, STR_TO_DATE(CSTG.order_date, '%Y-%m-%d') AS new_date_to
  FROM PRICES AS P
  JOIN PRODUCTS AS PRD ON PRD.product_id = P.product_id
  JOIN CSV_STAGING AS CSTG ON PRD.product_name = CSTG.product_name
  WHERE P.is_current = TRUE 
  AND CAST(CSTG.product_price AS DECIMAL(10, 2)) <> CAST(P.price AS DECIMAL(10, 2))
) AS to_close ON PRICES.product_id = to_close.product_id
SET PRICES.date_to = to_close.new_date_to,
    PRICES.is_current = FALSE;


-- STEP 2: ADD NEW PRICES
INSERT INTO PRICES (product_id, price, date_from, date_to, is_current)
SELECT 
    P.product_id,
    CSTG.product_price, 
    CSTG.order_date, 
    '9999-12-31',  -- Assuming '9999-12-31' or another far-future date signifies current record
    TRUE
FROM CSV_STAGING AS CSTG
LEFT JOIN PRODUCTS AS P ON P.product_name = CSTG.product_name
WHERE NOT EXISTS (
    SELECT 1
    FROM PRICES AS P2
    WHERE P2.product_id = P.product_id
    AND P2.is_current = TRUE
);


-- populating order fact table
INSERT INTO ORDERS_FACT(
    client_id,
    invoice_id,
    batch_id,
    branch_id,
    salesman_id,
    order_date,
    order_time,
    payment_method)
SELECT 
    CLIENT_PHONES.person_id, 
    CSV_STAGING.invoice_id, 
    @CURRENT_BATCH_ID, 
    BRANCHES.branch_id, 
    SALESMEN.salesman_id, 
    CSV_STAGING.order_date, 
    CSV_STAGING.order_time, 
    CSV_STAGING.payment_method
FROM
    CSV_STAGING
LEFT JOIN 
    CLIENT_PHONES ON CSV_STAGING.client_phone = CLIENT_PHONES.phone_number
LEFT JOIN 
    SALESMEN ON (
        CSV_STAGING.salesman_fname = SALESMEN.first_name
        AND CSV_STAGING.salesman_lname = SALESMEN.last_name
    )
LEFT JOIN
    BRANCHES ON BRANCHES.branch_name = CSV_STAGING.branch_name
WHERE (CSV_STAGING.invoice_id, BRANCHES.branch_id) NOT IN (
    SELECT invoice_id, branch_id FROM ORDERS_FACT
)
ON DUPLICATE KEY UPDATE
    client_id = VALUES(client_id),
    batch_id = VALUES(batch_id),
    salesman_id = VALUES(salesman_id),
    order_date = VALUES(order_date),
    order_time = VALUES(order_time),
    payment_method = VALUES(payment_method);




-- populate PRODUCT_ORDER
INSERT INTO PRODUCT_ORDER (invoice_id, product_id, order_amount)
SELECT 
    ORDERS_FACT.invoice_id, 
    PRODUCTS.product_id, 
    CSV_STAGING.amount
FROM 
    CSV_STAGING
LEFT JOIN 
    ORDERS_FACT ON ORDERS_FACT.invoice_id = CSV_STAGING.invoice_id
LEFT JOIN 
    PRODUCTS ON PRODUCTS.product_name = CSV_STAGING.product_name
WHERE 
    NOT EXISTS (
        SELECT 1 
        FROM PRODUCT_ORDER
        WHERE PRODUCT_ORDER.invoice_id = ORDERS_FACT.invoice_id
          AND PRODUCT_ORDER.product_id = PRODUCTS.product_id
          AND PRODUCT_ORDER.order_amount = CSV_STAGING.amount
    );



-- update ETL finish time in ETL_BATCH table 
UPDATE ETL_BATCH
SET finish_time = NOW() 
WHERE batch_id = @CURRENT_BATCH_ID;
