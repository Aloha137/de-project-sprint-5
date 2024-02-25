from logging import Logger
from typing import List
from datetime import date, datetime, time
from typing import Optional
import json
from core.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from core.dds.orders_repositories import OrderJsonObj, OrderRawRepository

from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class TimestampsObjStg(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime

class TimestampsObjDds(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date
                    # ON CONFLICT (ts) DO NOTHING;

class TimestampDdsRepository:
    # вставляем готовый timestamp в dds
    def insert_dds_timestamp(self, conn: Connection, timestamp: TimestampsObjDds) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date
                },
            )
# получить нужную строку в dds по timestamp
    def get_timestamp(self, conn: Connection, dt: datetime) -> Optional[TimestampsObjDds]:
        with conn.cursor(row_factory=class_row(TimestampsObjDds)) as cur:
            cur.execute(
                """
                    SELECT id, ts, year, month, day, time, date
                    FROM dds.dm_timestamps
                    WHERE ts = %(dt)s;
                """,
                {"dt": dt},
            )
            obj = cur.fetchone()
        return obj


class TimestampLoader:
    WF_KEY = "timestamp_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_order_id"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw_orders = OrderRawRepository()
        self.dds = TimestampDdsRepository()
        self.settings_repository = StgEtlSettingsRepository()

    # парсим дату заказа в поля для объекта строки timestamp
    def parse_order_ts(self, order_raw: OrderJsonObj) -> TimestampsObjDds:
        order_json = json.loads(order_raw.object_value)
        dt = datetime.strptime(order_json['date'], "%Y-%m-%d %H:%M:%S")
        t = TimestampsObjDds(id=0,
                            ts=dt,
                            year=dt.year,
                            month=dt.month,
                            day=dt.day,
                            time=dt.time(),
                            date=dt.date()
                            )

        return t

    def load_timestamps(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw_orders.load_raw_orders(conn, last_loaded_id)
            for order in load_queue:

                ts_to_load = self.parse_order_ts(order)
                self.dds.insert_dds_timestamp(conn, ts_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = order.id
                wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.

                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
