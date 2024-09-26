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
    * Obs: É possível adicionar mais workers no projeto alterando o docker-compose.yml.

* **postgres**: Postgres é utilizado pelo Airflow.
    * imagem: postgres:14.0

* **scheduler**: Airflow scheduler
    * imagem: apache/airflow:2.8.1-python3.9

* **jupyter-spark**: Utilizado para visualizar os dados na camada gold em formato de tabela.
    * imagem: jupyter/pyspark-notebook:x86_64-spark-3.5.0
    * porta: 8888

## Fluxo do Processo

![](https://github.com/Marcustap/BreweryListETL/blob/main/images/brewery_list_etl.PNG)

### Camada Bronze
Coleta de dados via API com política de retry e ingestão dos dados na camada bronze em arquivos em formato json.
Caminho: /datalake/bronze/breweries/{Ano}/{Mes}/{Dia}

### Camada Silver
Leitura dos arquivos da camada bronze, tratamento de dados e ingestão na camada silver em arquivos em formato parquet.
* Limpeza de caracteres especiais da coluna "state" que é utilizada como chave de particionamento da tabela.
* Filtragem de valores nulos para colunas chave.
* Normalização de valores da coluna state.

* Caminho: /datalake/silver/breweries/

* Schema:

| Coluna   | Data Type  |
|----------------|------------|
| id             | STRING     |
| name           | STRING     |
| brewery_type   | STRING     |
| street         | STRING     |
| address_2      | STRING     |
| address_3      | STRING     |
| city           | STRING     |
| postal_code    | STRING     |
| country        | STRING     |
| longitude      | STRING     |
| latitude       | STRING     |
| phone          | STRING     |
| website_url    | STRING     |
| state          | STRING     |
| dt_ingestion   | TIMESTAMP  |


### Camada Gold
Leitura dos dados presentes na camada silver e agregação dos dados apresentando a quantidade de cervejarias por localidade (coluna "state") dividias por tipos de cervejaria (coluna "brewery_type")

Caminho: /datalake/gold/breweries/num_breweries_per_state/

* Schema:

| Coluna            | Data Type  |
|-------------------|------------|
| state             | STRING     |
| brewery_type      | STRING     |
| brewery_quantity  | INTEGER    |
| dt_ingestion      | TIMESTAMP  |


### Data Quality
Entre os processos de ingestão existe o acionamento de scripts que verificam a integridade dos dados antes que eles sejam processados em camadas futuras, garantindo a confiabilidade nos dados que serão consumidos na camada gold.

## Testes
Na pasta **tests** no projeto há um exemplo de teste que pode ser executado pelo Airflow acionando a DAG **run_tests_dag**.
## Monitoramento e Alertas

Atualmente não há processos para monitoramento ativo e envio de alertas, entretanto seguem algumas sugestões para incrementar este tópico:
* Utilização de funcionalidade do Airflow para envio de e-mail em caso de falha no pipeline.
* Atualmente os logs do processo são realizados através da biblioteca logging do python. Uma melhoria importante seria a persistência dos logs em tabela adicionando informações como duração das etapas e quantidade de dados processados, facilitando o debug do processo quando for necessário.
* Caso a execução do pipeline demore mais tempo do que o aceitável para ser concluído, é necessário o envio de alerta imediato para a equipe responsável. Este cenário pode gerar custos desnecessário, além de impactar outros processos.

## Como executar o projeto

* Clonar o projeto:
    *  *git clone https://github.com/Marcustap/BreweryListETL*

* Ir até a pasta do projeto e executar o docker-compose com o comando:
    * *docker-compose up -d --build*

* Após alguns minutos, você deve estar com seis containeres criados e rodando, com exceção do container webserver do Airflow que deve estar com erro. Isto se dá por conta do postgress, basta reiniciar o container do webserver Airflow e o container irá apresentar o status Running normalmente.
    * *docker restart brewerylist-webserver-1*

* Com todos os containeres funcionando, vá até o seu navegador e acesse a página inicial do Airflow: <http://localhost:8080>. Coloque o usário e senha presentes do docker-compose.yml. Se você não alterou nada o usuário deve ser admin e senha admin.

* Em seguida, é necessário configurar a conexão do Airflow com o Spark. 
    * Na aba Admin, clique em connections;
      
    * ![](https://github.com/Marcustap/BreweryListETL/blob/main/images/Capturar.PNG)

    * Clique no botão de "+" - Add a new record;
 
      ![](https://github.com/Marcustap/BreweryListETL/blob/main/images/adicionar%20conexao.PNG)

    * Preencha os campos a seguir com os seguintes valores:
        
        * Connection Id: spark-con
        * Connection Type: Spark
        * Host: spark://spark-master
        * Port: 7077

      ![](https://github.com/Marcustap/BreweryListETL/blob/main/images/parametros_conexao.PNG)

    * Clique em Salvar

* Volte para a aba DAGs e você verá duas dags já criadas. A primeira "brewery_list_etl" contém o pipeline principal. Basta iniciar a DAG e aguardar a sua conclusão.

* Após a conclusão, acesso os logs do container Jupyter Notebook com o comando
  * *docker logs brewerylist-jupyter-spark-1*

* Acesse o servidor do jupyter notebook encontrando o link que inicia com  http://127.0.0.1:8888/lab?token... como na imagem abaixo:

  ![](https://github.com/Marcustap/BreweryListETL/blob/main/images/logs_jupyter_notebook.PNG)

* Agora basta executar o notebook na pasta work/notebooks/Reads Gold Layer.ipynb para acessar os dados.

  
 ![](https://github.com/Marcustap/BreweryListETL/blob/main/images/notebook.PNG)


