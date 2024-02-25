create schema if not exists cdm;

--DROP TABLE IF EXISTS cdm.dm_courier_ledger CASCADE;
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger
(id serial PRIMARY KEY,
courier_id integer NOT NULL,
courier_name varchar(255) NOT NULL,
settlement_year integer NOT NULL CHECK (settlement_year>='2022' AND settlement_year < '2500'),
settlement_month integer NOT NULL CHECK(((settlement_month >= '1') AND (settlement_month <= '12'))),
orders_count integer NOT NULL CHECK (orders_count >= 0) default (0),
orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= (0)::numeric),
rate_avg numeric(14,2) NOT NULL check (rate_avg >=0) default (0),
order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= (0)::numeric),
courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= (0)::numeric),
courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= (0)::numeric),
courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= (0)::numeric));

CREATE TABLE IF NOT EXISTS cdm.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

--DROP TABLE IF EXISTS cdm.dm_settlement_report CASCADE;
CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report
(id serial PRIMARY KEY,
restaurant_id integer NOT NULL,
restaurant_name varchar(255) NOT NULL,
settlement_date date NOT NULL CHECK (settlement_date>='2022-01-01' AND settlement_date < '2500-01-01'),
orders_count integer NOT NULL,
orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0,
orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0,
order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_count_check CHECK (orders_count >= 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK (orders_total_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK (orders_bonus_payment_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK (orders_bonus_granted_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK (order_processing_fee >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK (restaurant_reward_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_unique UNIQUE (restaurant_id, settlement_date);



