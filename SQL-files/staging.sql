USE DWH;

DROP TABLE IF EXISTS CSV_STAGING;

CREATE TABLE IF NOT EXISTS CSV_STAGING(
invoice_id          TEXT,
branch_name         TEXT,
city                TEXT,
client_fname        TEXT,
client_lname        TEXT,
salesman_fname      TEXT,
salesman_lname      TEXT,
client_email        TEXT,
client_phone        TEXT,
product_name        TEXT,
product_line        TEXT,
product_price       TEXT,
amount              TEXT,
order_date          TEXT,
order_time          TEXT,
payment_method      TEXT
);

LOAD DATA INFILE %s
INTO TABLE CSV_STAGING
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


