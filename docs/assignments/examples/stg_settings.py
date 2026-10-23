#файл надо положить dag/stg/stg_settings.py
from typing import Dict, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EtlSetting(BaseModel):
    id: int
    settings_key: str
    settings: Dict


class EtlStgSettings:
    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        settings_key,
                        settings
                    FROM stg.settings
                    WHERE settings_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, settings_key: str, settings: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.settings(settings_key, settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (settings_key) DO UPDATE
                    SET settings = EXCLUDED.settings;
                """,
                {
                    "etl_key": settings_key,
                    "etl_setting": settings
                },
            )
