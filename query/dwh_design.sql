-- Example DWH
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS fact_log;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_transaction;
DROP TABLE IF EXISTS dim_search;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_customer(
	customer_id BIGINT primary key,
	customer_name VARCHAR(255),
	customer_birthdate DATE,
	customer_gender VARCHAR(50),
	customer_country VARCHAR (255)
	);

CREATE TABLE dim_transaction(
	transaction_id BIGINT primary key,
	transaction_product VARCHAR(255)
	);

CREATE TABLE fact_log(
	log_id serial primary key,
	search_id BIGINT,
	search_date date,
	search_product VARCHAR(255)
	);

CREATE TABLE fact_sales(
	sales_id serial primary key,
	transaction_id int references dim_transaction,
	customer_id bigint references dim_customer,
	transaction_date DATE,
	transaction_amount INT
	);