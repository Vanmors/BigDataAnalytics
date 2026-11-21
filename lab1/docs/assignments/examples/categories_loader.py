from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettings
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CategoryObj(BaseModel):
    category_id: int
    name: str
    percent: float
    min_payment: float


class CategoriesSources:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def categories_list(self, category_bound: int, limit: int) -> List[CategoryObj]:
        with self._db.client().cursor(row_factory=class_row(CategoryObj)) as cur:
            cur.execute(
                """
                    SELECT category_id, name, percent, min_payment
                    FROM categories
                    WHERE category_id > %(boundary)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY category_id ASC --Обязательно сортируем по category_id, дальше будем использовать в курсоре.
                    LIMIT %(limit)s; --Обрабатываем только часть объектов.
                """, {
                    "boundary": category_bound,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class CategoryDest:

    def insert_category(self, conn: Connection, category: CategoryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.pg_category(category_id, name, percent, min_payment)
                    VALUES (%(category_id)s, %(name)s, %(bonus_percent)s, %(min_payment)s)
                    ON CONFLICT (category_id) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        bonus_percent = EXCLUDED.bonus_percent,
                        min_payment = EXCLUDED.min_payment;
                """,
                {
                    "category_id": category.category_id,
                    "name": category.name,
                    "bonus_percent": category.bonus_percent,
                    "min_payment": category.min_payment
                },
            )


class categoryLoader:
    OUR_STG_KEY = "example_categories_source_to_stg"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1  # Категорий мало, тут примерчик больше показательный, чтобы увидеть инкрементальную загрузку.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.source = CategoriesSources(pg_source)
        self.stg = CategoryDest()
        self.settings = StgEtlSettings()
        self.log = log

    def load_categories(self):
        # открываем транзакцию.
        # Транзакция commit, если код в блоке with собер1ётся без ошибок.
        # Если возникнет ошибка, произойдет откат всех изменений (rollback).
        with self.pg_dest.connection() as conn:

            # Если настройки еще нет, содаём её.
            stg_settings = self.settings.get_setting(conn, self.OUR_STG_KEY)
            if not stg_settings:
                stg_settings = EtlSetting(id=0, our_key=self.OUR_STG_KEY, our_settings={self.LAST_LOADED_ID_KEY: -1})

            # обращаемся к следующему батчу .
            last_loaded = stg_settings.our_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.categories_list(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} categories for load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем ву dwh.
            for category in load_queue:
                self.stg.insert_category(conn, category)

            # Сохраняем.
            # Используем connection, поэтому настройка сохранится вместе с объектами,
            # или откатятся все изменения.
            stg_settings.our_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            stg_settings_json = json2str(stg_settings.our_settings)  # Преобразуем в строку для сохранения в БД.
            self.settings.save_setting(conn, stg_settings.our_key, stg_settings_json)

            self.log.info(f"Load finished on {stg_settings.our_settings[self.LAST_LOADED_ID_KEY]}")
