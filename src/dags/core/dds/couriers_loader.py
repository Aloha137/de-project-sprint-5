import json
from datetime import datetime

from lib import PgConnect
from lib.dict_util import json2str
from typing import List, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from core.dds.dds_settings_repository import StgEtlSettingsRepository, EtlSetting

class CouriersLoader:
    # WF_KEY = "couriers_raw_to_dds_workflow"
    # LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect) -> None:
        self.dwh = pg

    def load_couriers(self):
        with self.dwh.connection() as conn:
            # wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            # if not wf_setting:
            #     wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            cur = conn.cursor()
            cur.execute(f"""
                INSERT INTO dds.dm_couriers (courier_id, courier_name)
                SELECT  object_id as courier_id , object_value::json ->> 'name' as courier_name
                FROM stg.couriers
                ON CONFLICT (courier_id) DO NOTHING;""")

            # load_queue = self.raw.load_raw_orders(conn, last_loaded_id)
            # load_queue.sort(key=lambda x: x.id)
            

            # wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = order_raw.id
            # wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.

            # self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)