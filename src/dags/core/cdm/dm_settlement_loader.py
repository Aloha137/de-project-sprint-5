from logging import Logger, getLogger

from lib import PgConnect
from psycopg import Connection
from pydantic import BaseModel
from datetime import datetime
from typing import Dict, List, Tuple
from lib.dict_util import json2str

from core.cdm.cdm_settings_repository import StgEtlSettingsRepository, EtlSetting

log = getLogger(__name__)

class DmSettlementLoader():
    WF_KEY = "load_cdm_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    _LOG_THRESHOLD = 100

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def max_ts(self,  conn: Connection):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select max(settlement_date)
                    from cdm.dm_settlement_report;
                    """)
                result = cur.fetchone()[0]
            self.log.info(f" Возвращаем max дату на витрине {result}")
            return result

    def prepare_cdm(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            print("На данный момент имеем следующие настройки ", str(wf_setting))
            if not wf_setting:
                wf_setting = EtlSetting(id=0,
                                        workflow_key=self.WF_KEY,
                                        workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()})
                print("Первый раз установили настройки", str(wf_setting))

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            log.info(f"Starting load from: {last_loaded}")
            with conn.cursor() as cur:
                cur.execute(
                """
                WITH order_sums AS (
                SELECT
                    r.id                      AS restaurant_id,
                    r.restaurant_name         AS restaurant_name,
                    tss.date                  AS settlement_date,
                    COUNT(DISTINCT orders.id) AS orders_count,
                    SUM(fct.total_sum)        AS orders_total_sum,
                    SUM(fct.bonus_payment)    AS orders_bonus_payment_sum,
                    SUM(fct.bonus_grant)      AS orders_bonus_granted_sum
                FROM dds.fct_product_sales as fct
                    INNER JOIN dds.dm_orders AS orders
                        ON fct.order_id = orders.id
                    INNER JOIN dds.dm_timestamps as tss
                        ON tss.id = orders.timestamp_id
                    INNER JOIN dds.dm_restaurants AS r
                        on r.id = orders.restaurant_id
                WHERE orders.order_status = 'CLOSED' and tss.date > %(last_loaded)s
                GROUP BY
                    r.id,
                    r.restaurant_name,
                    tss.date
            )

            INSERT INTO cdm.dm_settlement_report(
                restaurant_id,
                restaurant_name,
                settlement_date,
                orders_count,
                orders_total_sum,
                orders_bonus_payment_sum,
                orders_bonus_granted_sum,
                order_processing_fee,
                restaurant_reward_sum
            )

            SELECT
                restaurant_id,
                restaurant_name,
                settlement_date,
                orders_count,
                orders_total_sum,
                orders_bonus_payment_sum,
                orders_bonus_granted_sum,
                s.orders_total_sum * 0.25 AS order_processing_fee,
                s.orders_total_sum - s.orders_total_sum * 0.25 - s.orders_bonus_payment_sum AS restaurant_reward_sum
            FROM order_sums AS s
            ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
            SET
                orders_count = EXCLUDED.orders_count,
                orders_total_sum = EXCLUDED.orders_total_sum,
                orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                order_processing_fee = EXCLUDED.order_processing_fee,
                restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {"last_loaded": last_loaded},
                )

                last_loaded_ts = self.max_ts(conn)
            last_loaded_ts = datetime.strftime(last_loaded_ts, "%Y-%m-%d %H:%M:%S")
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded_ts

            self.log.info(f"Получили следующие настройки для записи {wf_setting.workflow_settings}")

            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")


                

