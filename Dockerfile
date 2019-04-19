FROM puckel/docker-airflow:1.10.1
ADD dags /usr/local/airflow/dags
ADD requirements.txt /usr/local/airflow
RUN pip install --user -r requirements.txt