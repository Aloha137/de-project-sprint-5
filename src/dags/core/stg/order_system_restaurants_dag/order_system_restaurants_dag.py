import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from core.stg.order_system_restaurants_dag.pg_saver import PgSaver
from core.stg.order_system_restaurants_dag.collection_loader import CollectionLoader
from core.stg.order_system_restaurants_dag.collection_copier import CollectionCopier
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_stg_order_system_restaurants():
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
    def load_users():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_loader = CollectionLoader(mongo_connect)
        copier = CollectionCopier(collection_loader, dwh_pg_connect, pg_saver, log)

        copier.run_copy('users')
        
    users_loader = load_users()

    @task()
    def load_orders():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_loader = CollectionLoader(mongo_connect)
        copier = CollectionCopier(collection_loader, dwh_pg_connect, pg_saver, log)

        copier.run_copy('orders')
        
    orders_loader = load_orders()



    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = CollectionLoader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CollectionCopier(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy('restaurants')

    restaurant_loader = load_restaurants()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    [users_loader,restaurant_loader,orders_loader]   # type: ignore


order_stg_dag = sprint5_example_stg_order_system_restaurants()  # noqa
