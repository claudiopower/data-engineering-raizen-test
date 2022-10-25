FROM apache/airflow:2.3.4
USER root
RUN apt-get update && apt-get install -y \
    && apt-get install libreoffice -y \
    && apt-get install -y locales \
    && localedef -i pt_BR -f UTF-8 pt_BR.UTF-8 

USER airflow
COPY ./requirements.txt ./
RUN pip install --upgrade pip \
    && pip install --upgrade setuptools \
    && pip install --user --no-cache-dir -r requirements.txt