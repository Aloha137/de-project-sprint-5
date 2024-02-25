from logging import Logger
from typing import List, Optional
from datetime import datetime

from core.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel



class RestaurantsObjStg(BaseModel):
    id: int
    object_id: str
    object_value: str

class RestaurantsObjDds(BaseModel):
    id: int
    restaurant_id: str #in DB text type
    restaurant_name: str
    update_ts: datetime

class RestaurantDdsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


# пригодится когда будем грузить продукты
class RestaurantRawRepository:
    def load_raw_restaurants(self, conn: Connection, last_loaded_record_id: int, log: Logger) -> List[RestaurantsObjStg]:
        with conn.cursor(row_factory=class_row(RestaurantsObjStg)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
            log.info(f"Мы получили следующие рестораны из stg для загрузки {objs} ")

        return objs

class RestaurantsStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, rank_threshold: int) -> List[RestaurantsObjDds]:
        with self._db.client().cursor(row_factory=class_row(RestaurantsObjDds)) as cur:
            cur.execute(
                """
                    SELECT
                        id, 
                        object_id :: varchar as restaurant_id,
	                   (object_value ::JSON ->> 'name') :: varchar as restaurant_name,
	                   (object_value ::JSON ->> 'update_ts') :: timestamp as update_ts
                    FROM stg.ordersystem_restaurants
                    where (object_value ::JSON ->> 'update_ts') :: timestamp > %(threshold)s
                """, {
                    "threshold": rank_threshold
                    }
            )
            objs = cur.fetchall()
            print(objs)
        return objs

class RestaurantsDdsRepository:

    def insert_restaurant(self, conn: Connection, restaurant: RestaurantsObjDds) -> None:
        with conn.cursor() as cur:
            future_date = "2099-12-31"
            print(  restaurant.restaurant_id,
                    restaurant.restaurant_name,
                    restaurant.update_ts,
                    future_date)
            cur.execute(
                """
                    SELECT active_to FROM dds.dm_restaurants WHERE restaurant_id = %(restaurant_id)s ORDER BY active_to DESC LIMIT 1;
                """,
                {"restaurant_id": restaurant.restaurant_id},
            )
            result = cur.fetchone()
            print('Выведем результат уже существуюшей строки в ресторанах: ',result)
            if result:
                cur.execute(
                    """
                        UPDATE dds.dm_restaurants 
                        SET active_to = %(update_ts)s
                        WHERE restaurant_id = %(restaurant_id)s AND active_to = %(future_date)s::timestamp ;
                    """,
                    {"restaurant_id": restaurant.restaurant_id,
                      "update_ts": restaurant.update_ts,
                      "future_date": future_date}
                )
            else:  
                cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s::timestamp)
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.update_ts,
                    "active_to": future_date
                }
            )
    # пригодится при загрузке продуктов
    def get_restaurant(self, conn: Connection, restaurant_id: str) -> Optional[RestaurantDdsObj]:
        with conn.cursor(row_factory=class_row(RestaurantDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        restaurant_id,
                        restaurant_name,
                        active_from,
                        active_to
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s;
                """,
                {"restaurant_id": restaurant_id},
            )
            obj = cur.fetchone()
        return obj



class RestaurantLoader:
    WF_KEY = "example_restaurants_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = None  # Загружается за один раз. Поэтому инкремент не нужен

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.stg = RestaurantsStgRepository(pg)
        self.dds = RestaurantsDdsRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, 
                                        workflow_key=self.WF_KEY, 
                                        workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            load_queue = self.stg.list_restaurants(last_loaded)
            print(load_queue)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.dds.insert_restaurant(conn, user)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")


