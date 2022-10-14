select distinct dim_transaction.transaction_id, dim_customer.customer_id, dim_customer.customer_name, dim_customer.customer_birthdate, dim_customer.customer_gender, dim_customer.customer_country, transaction_date, dim_transaction.transaction_product, transaction_amount
from fact_sales
join dim_customer on fact_sales.customer_id = dim_customer.customer_id
join dim_transaction on fact_sales.transaction_id = dim_transaction.transaction_id