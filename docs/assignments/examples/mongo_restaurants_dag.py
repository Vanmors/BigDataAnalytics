@dag(
    #  расписание 
    schedule_interval='* * * * *',
    # дата начала выполнения DAG
    start_date=pendulum.datetime(2024, 5, 24, tz="UTC"),
    # нужно ли запускать даг за предыдущие периоды (со start_date до сегодня). 
    catchup=False,  
    # Задаём теги, которые используются для фильтрации в интерфейсе Airflow. 
    tags=['example', 'stg', 'source'], 
        # остановлен DAG или запущен при создании.
    is_paused_upon_creation=False  # запущен.
)
def example_stg_mongo_restaurants():
    #  подключение к базе DWH.
    dwh_pg_connect = ConnectionBuilder.pg_conn("______")

    # обращаемся к переменным Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        #  класс для сохраниние.
        pg_saver = PgSaver()

        # подключение к MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # класс для чтения данных 
        collection_reader = RestaurantReader(mongo_connect)

        # класс для загрузки данных
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # копирование
        loader.run_copy()

    restaurant_loader = load_restaurants()

    #порядок выполнения. пока тут только одна задача, и мы не прописываем зависимости между задачами.
    restaurant_loader 

mongo_stg_dag = example_stg_mongo_restaurants() 