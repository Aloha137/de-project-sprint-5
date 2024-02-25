from datetime import datetime, timedelta
from logging import Logger
from typing import Dict, List

from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from core.stg import EtlSetting, StgEtlSettingsRepository


class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class UserRankJsonObj:
    EVENT_TYPE = "user_rank"

    def __init__(self, d: Dict) -> None:
        self.user_id: int = d["user_id"]
        self.rank_id: int = d["rank_id"]
        self.rank_name: str = d["rank_name"]
        self.rank_award: float = d["rank_award"]


class UserBalanceJsonObj:
    EVENT_TYPE = "user_balance"

    def __init__(self, d: Dict) -> None:
        self.user_id: int = d["user_id"]
        self.balance: float = d["balance"]


class ProductPaymentJsonObj:
    def __init__(self, d: Dict) -> None:
        self.product_id: str = d["product_id"]
        self.product_name: str = d["product_name"]
        self.price: float = d["price"]
        self.quantity: int = d["quantity"]
        self.product_cost: float = d["product_cost"]
        self.bonus_payment: float = d["bonus_payment"]
        self.bonus_grant: float = d["bonus_grant"]


class BonusPaymentJsonObj:
    EVENT_TYPE = "bonus_transaction"

    def __init__(self, d: Dict) -> None:
        self.user_id: int = d["user_id"]
        self.order_id: str = d["order_id"]
        self.order_date: datetime = datetime.strptime(d["order_date"], "%Y-%m-%d %H:%M:%S")
        self.product_payments = [ProductPaymentJsonObj(it) for it in d["product_payments"]]


class EventOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_events(self, last_loaded_record_id: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM outbox
                    WHERE event_ts > %(load_start)s AND id > %(last_loaded_record_id)s
                    ORDER BY id ASC;
                """,
                {
                    "last_loaded_record_id": last_loaded_record_id,
                    "load_start": datetime.utcnow() - timedelta(days=8)
                },
            )
            objs = cur.fetchall()
        return objs


class EventStgRepository:
    def save_events(self, conn: Connection, events: List[EventObj]) -> None:
        with conn.cursor() as cur:
            for event in events:
                cur.execute(
                    """
                        INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                        VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s);
                    """,
                    {
                        "id": event.id,
                        "event_ts": event.event_ts,
                        "event_type": event.event_type,
                        "event_value": event.event_value
                    },
                )


class EventLoader:
    WF_KEY = "events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.origin = EventOriginRepository(pg_origin)
        self.stg = EventStgRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self._log = log

    def load_events(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)

            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            self._log.info(f"Continuing from {last_loaded_id} event id.")

            load_queue = self.origin.load_events(last_loaded_id)
            if not load_queue:
                return

            self.stg.save_events(conn, load_queue)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, json2str(wf_setting.workflow_settings))
