create external table categories (
    category_id int,
    category_department_id int,
    category_name string
)
STORED AS PARQUET
LOCATION 's3://uwm-bigdata/data/sklep/categories/';


create external table customers (
    customer_id int,
    customer_fname string,
    customer_lname string,
    customer_email string,
    customer_password string,
    customer_street string,
    customer_city string,
    customer_state string,
    customer_zipcode string
)
STORED AS PARQUET
LOCATION 's3://uwm-bigdata/data/sklep/customers/';



create external table departments (
    department_id int,
    department_name string
)
STORED AS PARQUET
LOCATION 's3://uwm-bigdata/data/sklep/departments/';

create external table orders (
    order_id int,
    order_date bigint,
    order_customer_id int,
    order_status string
)
STORED AS PARQUET
LOCATION 's3://uwm-bigdata/data/sklep/orders/';

create external table order_items (
    order_item_id int,
    order_item_order_id int,
    order_item_product_id int,
    order_item_quantity int,
    order_item_subtotal float,
    order_item_product_price float
)
STORED AS PARQUET
LOCATION 's3://uwm-bigdata/data/sklep/order_items/';



create external table products (
    product_id int,
    product_category_id int,
    product_name string,
    product_description string,
    product_price float,
    product_image string
)
STORED AS PARQUET
LOCATION 's3://uwm-bigdata/data/sklep/products/';

