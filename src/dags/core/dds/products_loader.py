from logging import Logger
from typing import List
from datetime import date, datetime, time
from typing import Optional
import json
from core.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from core.dds.orders_loader import OrderJsonObj, OrderRawRepository
from core.dds.restaurants_load import (RestaurantsDdsRepository, RestaurantsObjStg,
                                   RestaurantRawRepository,RestaurantDdsObj)

from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ProductDdsObj(BaseModel):
    id: int

    product_id: str
    product_name: str
    product_price: float

    active_from: datetime
    active_to: datetime

    restaurant_id: int

class ProductDdsRepository:
    # вставляем итоговый данные в dds
    def insert_dds_products(self, conn: Connection, products: List[ProductDdsObj], log: Logger) -> None:
        with conn.cursor() as cur:
            for product in products:
                cur.execute(
                    """
                        INSERT INTO dds.dm_products(
                            product_id,
                            product_name,
                            product_price,
                            active_from,
                            active_to,
                            restaurant_id)
                        VALUES (
                            %(product_id)s,
                            %(product_name)s,
                            %(product_price)s,
                            %(active_from)s,
                            %(active_to)s,
                            %(restaurant_id)s);
                    """,
                    {
                        "product_id": product.product_id,
                        "product_name": product.product_name,
                        "product_price": product.product_price,
                        "active_from": product.active_from,
                        "active_to": product.active_to,
                        "restaurant_id": product.restaurant_id
                    },
                )
                log.info(f"Продукт {product} успешно загружен")

# получить определенный продукт по id 
    def get_product(self, conn: Connection, product_id: str) -> Optional[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products
                    WHERE product_id = %(product_id)s;
                """,
                {"product_id": product_id},
            )
            obj = cur.fetchone()
        return obj

    def list_products(self, conn: Connection) -> List[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products;
                """
            )
            obj = cur.fetchall()
        return obj

class ProductLoader:
    WF_KEY = "menu_products_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw = RestaurantRawRepository()
        self.dds_products = ProductDdsRepository()
        self.dds_restaurants = RestaurantsDdsRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def parse_restaurants_menu(self, restaurant_raw: RestaurantsObjStg, restaurant_version_id: int) -> List[ProductDdsObj]:
        res = []
        rest_json = json.loads(restaurant_raw.object_value)
        for prod_json in rest_json['menu']:
            t = ProductDdsObj(id=0,
                              product_id=prod_json['_id'],
                              product_name=prod_json['name'],
                              product_price=prod_json['price'],
                              active_from=datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                              active_to=datetime(year=2099, month=12, day=31),
                              restaurant_id=restaurant_version_id
                              )

            res.append(t)
            self.log.info(f"Мы получили объект продукт для загрузки {res} ")
        return res

    def load_products(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            #загружаем из stg все объекты ресторанов, смотря на метку id уже загруженных
            # здесь мы уже имеем и ресторан и продукты 
            load_queue = self.raw.load_raw_restaurants(conn, last_loaded_id, self.log)
            load_queue.sort(key=lambda x: x.id)

            # загружаем из dds имеющиеся объекты продуктов
            products = self.dds_products.list_products(conn)
            # получаем словарь {product_id: ProductDdsObj }
            prod_dict = {}
            for p in products:
                prod_dict[p.product_id] = p
            # грузим рестораны из уже загруженных в dds по нужным id из load_queue
            for restaurant in load_queue:
                # если нашлось значит среди новых загружаемых данных уже есть в dds этот ресторан
                restaurant_version = self.dds_restaurants.get_restaurant(conn, restaurant.object_id)
                if not restaurant_version:
                    return
                 #парcим объект продукты из объекта ресторан загружаемого и id ресторана уже 
                 #существующего в dds 
                products_to_load = self.parse_restaurants_menu(restaurant, restaurant_version.id)
                # создаем список продуктов при условии, что их еще нет в dds (prod_dict)
                products_to_load = [p for p in products_to_load if p.product_id not in prod_dict]
                self.dds_products.insert_dds_products(conn, products_to_load, self.log)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = restaurant.id
                wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

