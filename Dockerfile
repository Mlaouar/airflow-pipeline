FROM apache/airflow:2.5.2-python3.10
COPY requirements.txt .
RUN pip install -r requirements.txt