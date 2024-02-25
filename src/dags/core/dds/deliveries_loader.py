import json
from datetime import datetime
from logging import Logger

from lib import PgConnect
from lib.dict_util import json2str
from typing import List, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from core.dds.dds_settings_repository import StgEtlSettingsRepository, EtlSetting


class DeliveriesLoader:
    WF_KEY = "deliveries_raw_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()


    def load_deliveries(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_TS_KEY:datetime(2022, 1, 1).isoformat()})

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            cur = conn.cursor()
            cur.execute(f"""select
                        object_value::JSON ->> 'delivery_id'               as delivery_id,
                        object_value::JSON ->> 'order_id'                  as order_id,
                        object_value::JSON ->> 'order_ts'                  as order_ts,
                        object_value::JSON ->> 'courier_id'                as courier_id,
                        object_value::JSON ->> 'address'                   as address,
                        (object_value::JSON ->> 'delivery_ts')::timestamp  as delivery_ts,
                        (object_value::JSON ->> 'rate')::int               as rate,
                        (object_value::JSON ->> 'sum')::int               as order_sum,
                        (object_value::JSON ->> 'tip_sum')::numeric(14,2)  as tip_sum  FROM stg.deliveries;""")
            load_queue = cur.fetchall()
            new_dates = []
            for row in load_queue:
                order_id = row[1]
                order_ts = row[2]
                delivery_id = row[0]
                courier_id = row[3]
                address = row[4]
                delivery_ts = row[5]
                rate=row[6]
                order_sum = row[7]
                tip_sum = row[8]
                new_dates.append(delivery_ts)
                delivery_insert = f"""INSERT INTO dds.dm_delivery (order_id, delivery_id, courier_id, address, delivery_ts_id, rate, order_sum, tip_sum)
                            SELECT delivery_info.order_id as order_id, delivery_info.delivery_id as delivery_id, dc.id as courier_id,
                            delivery_info.address as address, dt.id as delivery_ts_id, delivery_info.rate::integer as rate,
                            delivery_info.order_sum::numeric(14,6) as order_sum, delivery_info.tip_sum::numeric(14,6) as tip_sum
                            FROM (
                                SELECT '{order_id}' as order_id, '{order_ts}' as order_ts, '{delivery_id}' as delivery_id, '{courier_id}' as courier_id,
                                        '{address}' as address, '{delivery_ts}' as delivery_ts, '{rate}' as rate, '{order_sum}' as order_sum, '{tip_sum}' as tip_sum
                            ) as delivery_info
                            INNER JOIN dds.dm_timestamps dt
                            ON Date_trunc('second', dt.ts) = Date_trunc('second', delivery_info.delivery_ts::timestamptz)
                            INNER JOIN dds.dm_couriers dc
                            ON dc.courier_id = delivery_info.courier_id
                            WHERE delivery_info.delivery_ts > '{last_loaded_ts}';"""
                cur.execute(delivery_insert)

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(new_dates)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")


            # cur = conn.cursor()
            # cur.execute(f"""
            #     INSERT INTO dds.dm_couriers (courier_id, courier_name)
            #     SELECT  object_id as courier_id , object_value::json ->> 'name' as courier_name
            #     FROM stg.couriers
            #     ON CONFLICT (courier_id) DO NOTHING;""")

            # load_queue = self.raw.load_raw_orders(conn, last_loaded_id)
            # load_queue.sort(key=lambda x: x.id)
            

            # wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = order_raw.id
            # wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.

            # self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)