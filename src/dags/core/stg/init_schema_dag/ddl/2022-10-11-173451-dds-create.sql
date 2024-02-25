create schema if not exists dds;

CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
id serial UNIQUE NOT NULL,
restaurant_id varchar NOT NULL UNIQUE,
restaurant_name  varchar NOT NULL,
active_from  timestamp NOT NULL,
active_to timestamp NOT NULL
);


CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
id serial PRIMARY KEY,
ts timestamp NOT NULL,
year smallint NOT NULL CHECK(((year >= 2022) AND (year < 2500))),
month smallint NOT NULL CHECK(((month >= 1) AND (month <= 12))),
day smallint NOT NULL CHECK(((day >= 1) AND (day <= 31))),
time time NOT NULL,
date date NOT NULL,
special_mark int DEFAULT 0);
-- special_mark потом может пригодиться

CREATE TABLE IF NOT EXISTS dds.dm_users
(
id serial UNIQUE NOT NULL,
user_id varchar NOT NULL,
user_name  varchar NOT NULL,
user_login  varchar NOT NULL UNIQUE);


CREATE TABLE IF NOT EXISTS dds.dm_couriers (
id serial UNIQUE NOT NULL,
courier_id varchar NOT NULL UNIQUE,
courier_name  varchar NOT NULL);


CREATE TABLE IF NOT EXISTS dds.dm_delivery (
id serial PRIMARY KEY,
order_id varchar NOT NULL,
delivery_id varchar NOT NULL,
courier_id integer NOT NULL,
address varchar NOT NULL,
delivery_ts_id integer NOT NULL,
rate float NOT NULL,
order_sum numeric(14,6),
tip_sum numeric(14,6));
--CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id) ON UPDATE CASCADE,
--CONSTRAINT fk_courier FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id) ON UPDATE CASCADE);

ALTER TABLE dds.dm_delivery  DROP CONSTRAINT IF EXISTS fk_courier;
ALTER TABLE dds.dm_delivery ADD CONSTRAINT fk_courier  FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id) ON UPDATE CASCADE;


ALTER TABLE dds.dm_products  DROP CONSTRAINT IF EXISTS dm_products_restaurant_id_fkey;

CREATE TABLE IF NOT EXISTS dds.dm_products (
id serial UNIQUE NOT NULL,
restaurant_id integer NOT NULL,
product_id varchar NOT NULL,
product_name varchar NOT NULL,
product_price numeric(14,2) NOT NULL DEFAULT 0 CHECK (product_price>=0 and product_price<=999000000.99),
active_from  timestamp NOT NULL,
active_to timestamp NOT NULL
);


CREATE TABLE IF NOT EXISTS dds.dm_orders (
id serial PRIMARY KEY,
user_id integer NOT NULL,
restaurant_id integer NOT NULL,
timestamp_id integer NOT NULL,
order_key varchar UNIQUE NOT NULL,
order_status varchar NOT NULL);

ALTER TABLE dds.dm_orders  DROP CONSTRAINT IF EXISTS dm_orders_dm_restaurants_fk;
ALTER TABLE dds.dm_orders  DROP CONSTRAINT IF EXISTS dm_orders_dm_timestamps_fk;
ALTER TABLE dds.dm_orders  DROP CONSTRAINT IF EXISTS dm_orders_dm_users_fk;

ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_dm_restaurants_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id) ON UPDATE CASCADE;
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_dm_timestamps_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id) ON UPDATE CASCADE;
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_dm_users_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users(id) ON UPDATE CASCADE;


CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);



CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
id serial PRIMARY KEY,
product_id integer NOT NULL,
order_id integer NOT NULL,
count integer NOT NULL DEFAULT 0 CHECK(count>=0),
price numeric(14,2) NOT NULL DEFAULT 0 CHECK(price>=0),
total_sum numeric(14,2) NOT NULL DEFAULT 0 CHECK(total_sum>=0),
bonus_payment numeric(14, 2) NOT NULL DEFAULT 0 CHECK(bonus_payment>=0),
bonus_grant numeric(14, 2) NOT NULL DEFAULT 0 CHECK(bonus_grant>=0));

ALTER TABLE dds.fct_product_sales  DROP CONSTRAINT IF EXISTS fct_product_sales_dm_products_fk;
ALTER TABLE dds.fct_product_sales  DROP CONSTRAINT IF EXISTS fct_product_sales_dm_orders_fk;

ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_dm_products_fk FOREIGN KEY (product_id) REFERENCES dds.dm_products(id) ON UPDATE CASCADE;
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_dm_orders_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id) ON UPDATE CASCADE;
ALTER TABLE dds.fct_product_sales ADD UNIQUE (order_id, product_id);


