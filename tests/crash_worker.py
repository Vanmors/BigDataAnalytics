"""Hard process exit at the PG commit boundary, only in isolated acceptance tests."""
import os
import psycopg2
from psycopg2.extensions import connection
import task3_mongo_cdc as cdc


class CrashConnection(connection):
    def commit(self):
        if os.environ['TASK3_CRASH_POINT'] == 'before':
            os._exit(91)
        super().commit()
        os._exit(92)


pg = psycopg2.connect(os.environ['TASK3_PG_DSN'], connection_factory=CrashConnection)
with cdc.mongo_client(os.environ['TASK3_MONGO_URI']) as client:
    cdc.run(pg, client, os.environ['TASK3_MONGO_DATABASE'], os.environ.get('TASK3_COMMAND', 'consume'), pause_held=True)
