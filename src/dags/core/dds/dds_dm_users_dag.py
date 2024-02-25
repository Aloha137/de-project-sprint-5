import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from core.dds.users_loader import  UserLoader 
from core.dds.restaurants_load import RestaurantLoader 
from core.dds.timestamp_loader import TimestampLoader 
from core.dds.products_loader import ProductLoader 
from core.dds.couriers_loader import CouriersLoader 
from core.dds.deliveries_loader import DeliveriesLoader 


from core.dds.orders_loader import OrderLoader 
from core.dds.fct_product_loader import FctProductsLoader 

from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_stg_to_dds_users():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants_to_dds():
        dds = RestaurantLoader(dwh_pg_connect,log)
        dds.load_restaurants()

    restaurants_loader = load_restaurants_to_dds()

    @task()
    def load_timestamps_to_dds():
        dds = TimestampLoader(dwh_pg_connect,log)
        dds.load_timestamps()

    timestamps_loader = load_timestamps_to_dds()

    @task()
    def load_users_to_dds():
        dds = UserLoader(dwh_pg_connect,log)
        dds.load_users()

    users_loader = load_users_to_dds()


    @task()
    def load_products_to_dds():
        dds = ProductLoader(dwh_pg_connect,log)
        dds.load_products()

    products_loader = load_products_to_dds()

    @task()
    def load_couriers_to_dds():
        dds = CouriersLoader(dwh_pg_connect)
        dds.load_couriers()

    couriers_loader = load_couriers_to_dds()

    @task()
    def load_deliveries_to_dds():
        dds = DeliveriesLoader(dwh_pg_connect, log)
        dds.load_deliveries()

    deliveries_loader = load_deliveries_to_dds()

    @task()
    def load_orders_to_dds():
        dds = OrderLoader(dwh_pg_connect)
        dds.load_orders()

    orders_loader = load_orders_to_dds()

    @task()
    def load_facts_to_dds():
        dds = FctProductsLoader(dwh_pg_connect)
        dds.load_product_facts()

    facts_loader = load_facts_to_dds()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    # [,restaurants_loader, timestamps_loader] >> >> orders_loader >> facts_loader   # type: ignore
    [restaurants_loader, timestamps_loader, users_loader, products_loader] >> couriers_loader >> deliveries_loader >> orders_loader >> facts_loader

users_dds_dag = sprint5_example_stg_to_dds_users()  # noqa
