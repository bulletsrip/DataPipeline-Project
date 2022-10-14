truncate table dim_customer CASCADE;
truncate table dim_transaction CASCADE;
truncate table fact_sales CASCADE;
truncate table fact_log CASCADE;

insert into dim_customer (customer_id, customer_name, customer_birthdate, customer_gender, customer_country)
select t1.*
from dblink('hostaddr=127.0.0.1 dbname=data_sources user=postgres password=123456', 'select distinct id_customer, name_customer, birthdate_customer, gender_customer, country_customer from bigdata_customer')
	as t1(id_customer bigint,
	name_customer text,
	birthdate_customer date,
	gender_customer text,
	country_customer text);

insert into dim_transaction (transaction_id, transaction_product)
select t2.*
from dblink('hostaddr=127.0.0.1 dbname=data_sources user=postgres password=123456', 'select distinct id_transaction, product_transaction from bigdata_transaction')
	as t2(id_transaction bigint,
	product_transaction text);

insert into fact_sales (transaction_id, customer_id, transaction_date, transaction_amount)
select t5.*
from dblink('hostaddr=127.0.0.1 dbname=data_sources user=postgres password=123456', 'select id_transaction, id_customer, date_transaction, amount_transaction from bigdata_transaction')
	as t5(id_transaction bigint,
	id_customer bigint,
	date_transaction date,
	amount_transaction int);

insert into fact_log (search_id, search_date, search_product)
select t3.*
from dblink('hostaddr=127.0.0.1 dbname=data_sources user=postgres password=123456', 'select id_search, date_search, product_search from bigdata_log')
	as t3(id_search bigint,
	search_date date,
	product_search text);