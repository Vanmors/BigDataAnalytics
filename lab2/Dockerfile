FROM apache/airflow:2.10.5-python3.12
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install --no-cache-dir --index-url "${PIP_INDEX_URL}" "apache-airflow==2.10.5" \
    "apache-airflow-providers-vertica==4.1.4" "vertica-python==1.4.0" && pip check
RUN mkdir -p /opt/airflow/data /opt/airflow/logs
