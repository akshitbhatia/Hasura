FROM apache/airflow:2.2.2
COPY ./requirements.txt .
RUN python3 -m pip install --upgrade pip
RUN pip install pandas==1.3.4 pandasql==0.7.3