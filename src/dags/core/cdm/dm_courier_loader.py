from logging import Logger, getLogger

from lib import PgConnect
from psycopg import Connection
from pydantic import BaseModel
from datetime import datetime
from typing import Dict, List, Tuple
from lib.dict_util import json2str

from core.cdm.cdm_settings_repository import StgEtlSettingsRepository, EtlSetting

log = getLogger(__name__)

class DmCourierLoader():
    WF_KEY = "load_cdm_courier_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    _LOG_THRESHOLD = 100

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def prepare_cdm(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            print("На данный момент имеем следующие настройки ", str(wf_setting))
            if not wf_setting:
                wf_setting = EtlSetting(id=0,
                                        workflow_key=self.WF_KEY,
                                        workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1)})
                print("Первый раз установили настройки", str(wf_setting))

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            log.info(f"Starting load from: {last_loaded}")
            with conn.cursor() as cur:
                cur.execute(
                """
                INSERT INTO cdm.dm_courier_ledger
                    (courier_id, courier_name, settlement_year,
                    settlement_month, orders_count, orders_total_sum,
                    rate_avg, order_processing_fee, courier_order_sum,
                    courier_tips_sum, courier_reward_sum)
                with cte AS (
                    SELECT
                    dd.courier_id,
                    dd.delivery_ts_id,
                    dd.rate,
                    dd.order_sum,
                    dt.year,
                    dt.month,
                    case
                    WHEN dd.rate < 4 and dd.order_sum * 0.05 > 100
                    then dd.order_sum * 0.05
                    WHEN dd.rate < 4 and dd.order_sum * 0.05 < 100
                    then 100
                    WHEN rate < 4.5 and dd.order_sum * 0.07 > 150
                    then dd.order_sum * 0.07
                    WHEN dd.rate < 4.5 and dd.order_sum * 0.07 < 150
                    then 150
                    WHEN dd.rate < 4.9 and dd.order_sum * 0.08 > 175
                    then dd.order_sum * 0.08
                    WHEN dd.rate < 4.9 and dd.order_sum * 0.08 < 175
                    then 175
                    WHEN dd.rate >= 4.9 and dd.order_sum * 0.1 > 200
                    then dd.order_sum * 0.1
                    else 200
                    END as reward,
                    tip_sum
                    FROM dds.dm_delivery dd
                    INNER JOIN dds.dm_timestamps dt
                    ON dd.delivery_ts_id = dt.id
                    where dt.month=(select date_part('month', (now()-interval '1 month')))
                    )
                    SELECT
                    dc.id as courier_id,
                    max(dc.courier_name) as courier_name,
                    max(cte.year) as year,
                    max(cte.month) as month,
                    count(1) as orders_count,
                    sum(cte.order_sum) as orders_total_sum,
                    avg(cte.rate) as rate_avg,
                    sum(cte.order_sum) * 0.25 as order_processing_fee,
                    sum(cte.reward) as courier_order_sum,
                    sum(tip_sum) as courier_tips_sum,
                    (sum(cte.reward) + sum(tip_sum)) * 0.95 as courier_reward_sum
                    from cte
                    inner join dds.dm_couriers dc
                    on cte.courier_id = dc.id
                    group by dc.id
                    """
                    )

            last_loaded_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded_ts

            self.log.info(f"Получили следующие настройки для записи {wf_setting.workflow_settings}")

            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")


                

