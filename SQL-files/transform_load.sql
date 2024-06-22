USE DWH;

SET @CURRENT_BATCH_ID = (
    SELECT
      COALESCE(MAX(batch_id), 0) AS next_id
    FROM
      ETL_BATCH
    WHERE (start_time IS NOT NULL) AND (finish_time IS NOT NULL)
  ) + 1;

INSERT INTO ETL_BATCH (batch_id, start_time)
VALUES (@CURRENT_BATCH_ID, NOW()); 

INSERT INTO BRANCHES(branch_name, city)
SELECT DISTINCT branch_name, city
FROM CSV_STAGING
WHERE (branch_name, city) NOT IN (
    SELECT branch_name, city FROM BRANCHES
    );

INSERT INTO CLIENTS (first_name, last_name)
SELECT DISTINCT client_fname, client_lname
FROM CSV_STAGING
WHERE (client_fname, client_lname) NOT IN (
    SELECT first_name, last_name
    FROM CLIENTS
) 
AND client_phone NOT IN (SELECT phone_number FROM CLIENT_PHONES)
AND client_email NOT IN (SELECT email FROM CLIENT_EMAILS);

INSERT INTO CLIENT_EMAILS (person_id, email)
SELECT DISTINCT CLIENTS.client_id, CSV_STAGING.client_email
FROM CSV_STAGING
LEFT JOIN CLIENTS ON CLIENTS.first_name = CSV_STAGING.client_fname 
AND CLIENTS.last_name = CSV_STAGING.client_lname
WHERE CSV_STAGING.client_email IS NOT NULL
AND CSV_STAGING.client_email NOT IN (
    SELECT email FROM CLIENT_EMAILS
);

INSERT INTO CLIENT_PHONES (person_id, phone_number)
SELECT DISTINCT CLIENTS.client_id, CSV_STAGING.client_phone
FROM CSV_STAGING
JOIN CLIENTS ON CLIENTS.first_name = CSV_STAGING.client_fname 
AND CLIENTS.last_name = CSV_STAGING.client_lname
WHERE CSV_STAGING.client_phone IS NOT NULL
AND CSV_STAGING.client_phone NOT IN (
    SELECT phone_number FROM CLIENT_PHONES
);

INSERT INTO SALESMEN (first_name, last_name)
SELECT DISTINCT salesman_fname, salesman_lname
FROM CSV_STAGING
WHERE CSV_STAGING.salesman_fname NOT IN (SELECT first_name FROM SALESMEN)
AND CSV_STAGING.salesman_lname   NOT IN (SELECT last_name FROM SALESMEN);


-- SCD2: PRODUCTS
-- STEP ONE: INSERT NEW PRODUCTS (OR) NEW PRICES
--      THIS QUERY HANDLES THE FOLLOWING SENARIOS:
--          1. INSERTING NEW PRODUCTS
--          2. INSERTING EXISTING PRODUCTS WITH OLD PRICES (IN THE SAME BATCH)
--          3. INSERTING EXISTING PRODUCTS WITH NEW PRICES (IN THE SAME BATCH)
INSERT INTO PRODUCTS (
    product_name,
    product_line,
    price,
    date_from,
    date_to,
    is_current
)
SELECT DISTINCT
    staging.product_name,
    staging.product_line,
    staging.product_price,
    '9999-12-31',
    staging.order_date,
    CASE 
        WHEN staging.order_date > (
            SELECT MAX(date_to)
            FROM PRODUCTS
            WHERE product_name = staging.product_name
        )
        THEN TRUE
        ELSE FALSE
    END
FROM CSV_STAGING AS staging
WHERE NOT EXISTS (
    SELECT 1
    FROM PRODUCTS p
    WHERE p.product_name = staging.product_name
      AND p.product_line = staging.product_line
      AND p.price = staging.product_price
);
-- STEP 2: UPDATE date_to FOR OLD PRICES TO CURRENT DATE
UPDATE PRODUCTS P 
JOIN (
    SELECT
        product_name,
        date_from,
        LEAD(date_from) OVER (PARTITION BY product_name ORDER BY date_from) AS end_date
    FROM PRODUCTS
) AS CTE ON P.product_name = CTE.product_name AND P.date_from = CTE.date_from
SET 
    P.date_to = CTE.end_date,
    P.is_current = FALSE
WHERE 
    CTE.end_date IS NOT NULL;


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



UPDATE ETL_BATCH
SET finish_time = NOW() 
WHERE batch_id = @CURRENT_BATCH_ID;
