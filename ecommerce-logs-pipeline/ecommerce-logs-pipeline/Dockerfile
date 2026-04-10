FROM apache/airflow:2.8.0

# Bascule en root pour installer les paquets de l'OS
USER root

# Installation des compilateurs et de la librairie Kerberos
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         g++ \
         libkrb5-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Retour à l'utilisateur airflow pour installer les paquets Python
USER airflow

# Installation "en dur" des providers
RUN pip install --no-cache-dir requests apache-airflow-providers-apache-hdfs