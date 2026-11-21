FROM apache/airflow:2.10.5-python3.12
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install --no-cache-dir --index-url "${PIP_INDEX_URL}" "apache-airflow==2.10.5" "pymongo==4.11" && pip check
RUN mkdir -p /opt/airflow/data /opt/airflow/logs
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root sql/ /opt/airflow/sql/
COPY --chown=airflow:root plugins/task3_mongo_cdc.py /opt/airflow/plugins/task3_mongo_cdc.py
COPY --chown=airflow:root mongo/ /opt/airflow/mongo/
COPY --chown=airflow:root data/generate_data.py /opt/airflow/data/generate_data.py
COPY --chown=airflow:root data/task3/ /opt/airflow/data/task3/
COPY --chown=airflow:root scripts/demo.py /opt/airflow/demo.py
