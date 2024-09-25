# BreweryListETL

## Sobre o Projeto

Este projeto tem por objetivo coletar dados de cervejarias através da API disponível em <https://api.openbrewerydb.org/breweries>, transformá-los e inseri-los em um Data Lake seguindo conceitos de arquitetura Medalhão (camadas bronze, silver e gold).

## Containeres Docker

* **airflow-webserver**:
    * imagem: apache/airflow:2.8.1-python3.9
    * porta: 8080

* **spark-master**:
    * imagem: bitnami/spark:3.5.0-debian-11-r0
    * porta: 9090

* **spark-worker**:
    * imagem: bitnami/spark:3.5.0-debian-11-r0
