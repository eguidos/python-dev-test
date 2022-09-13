# Extração, transformação, e ingestão de dados em ambiente de Data WareHouse

## Conteúdo deste artigo
1. Resumo;
2. Solução:
    1. Desenvolvimento do componente de extração de dados.
    2. ETL de dados com Apache Spark / Proposta de implementação do modelo Data Warehouse.
    3. Proposta de schedulagem do pipeline de dados.
3. Execução;
4. Conclusão;

* Resumo
    * O conteúdo deste pipeline de **ETL** tem como objetivo demonstrar o fluxo de execução de execução de dados, implementando regras de negócio utlizando como base **Apache Spark**. Além da aplicação de transformações para viabilizar a implementação das transformações (*que também podem conter regras de negócios*), os dados foram desnormalizados seguindo os conceitos do modelo **Star Schema (KIMBALL)**.
    * Tecnologias que viabilizaram a realização do case:
        * [Apache Airflow](https://airflow.apache.org/)
        * [Postgres](https://www.postgresql.org/)
        * [Apache Pyspark](https://spark.apache.org/docs/latest/api/python/)
    



* **Solução**
    * Para este case, foi-se desenvolvido um módulo de transformações com o framework pyspark, seguindo as orientações do [README](https://github.com/clicksign/python-dev-test/blob/master/README.md) enviado pela organização. Sendo assim, a implementação da normalização de dados se adere totalmente à [DESCRIÇÃO](https://github.com/clicksign/python-dev-test/blob/master/data/Description) de como os dados deverão ser tratados (data cleasing, relacionemtos, padronização de nome de colunas e etc.).


* **ETL de dados com apache Apache Spark  e Proposta de implementação do modelo Data Warehouse.**
    * A estrutura necessária para a execução do pipeline de ETL foi implementada seguindo o pradão semelhante a Arquitetura Orientada a Serviçõs (SOA) evidenciado abaixo. Este padrão contribui para fácil manutenção do código fonte e principalmente para escalabilidade de análises sob o dado coletado.
    ![DATA](/resources/project_model.png)

### Sources
        Contém as fontes de dados disponibilizadas para o desenvolvimento do case.

### RAW:
    Módulo que faz o processo de extração e salva o dado bruto (sem transformação, regras e etc) na tabela *raw_atult*

### Integration
    Módulo que faz o processo de transformação e salva o dado já transformado na tablea *intatult*. A partir desta etapa, os dados já podem ser consumidos no banco postgres.

### Business
    Módulo de desenvolvimento que implementa o modelo Data Warehouse e disponibiliza a tabela *bs_fact_adult* para análise.

### DAO
    Modulo que viabiliza a escrita / leitura das tabelas no postgress.


### Proposta de implementação do modelo Data Warehouse
    O modelo relacional foi implementado sob o banco relacional Postgres Versão 4.0, para Linux conforme o ilustrado abaixo.
![MER](/resources/data_modeling.png)

1. Tabelas Dimensionais:
    1. bs_dim_class: 
        1. Dimensão que contém informações sobre classe soci-oeconômica.
    2. bs_dim_education:
        1. Dimensão que contém informações sobre a escolaridade de cada indivíduo.
    3. bs_dim_marital_status:
        1. Dimensão que contém informações referentes a estado civíl.
    4. bs_dim_native_country:
        1. Dimensão que contém nomes de países.
    5. bs_dim_occupation:
        1. Dimensão que contém profissões.
    6. bs_dim_race:
        1. Dimensão que contém raça.
    7. bs_dim_relationship:
        1. Dimensão que contém status de relacionamento.
    8. bs_dim_sex:
        1. Dimensão que armazena sexo no qual o indivídeo se identifica.
    9. bs_dim_time:
        1. Dimensão que armazena tempo
    10. bs_dim_workclass:
        1. Dimensão que armazena o statua profissional.

2. Tabela Fato:
    1. bs_fact_adult:
        1. Tabela fato que armazena a granularidade mínima dos códigos dimensionais e também, dados de capital ganho, perdido, idade, horas trabalhadas por semana.
        2. Na proposta original, uma das solicitações seria o processamento a cada 10 segundos, processando **1.630 registros**, porém neste pipeline, foi viabilizado o modelo de *DATA WAREHOUSE*, onde os dados são armazenados à fim de analizar eventos, fatos, informaçoes e até identificar padrões de opiniões de acordo com o tempo.

### Proposta de schedulagem do pipeline de dados.

    O serviço de schedulagem foi desenvolvido por meio do Apache Airflow, dividido entre tasks, conforme descreve a imagem abaixo. As dags desenvovlidas estão disponíveis no módulo airflow deste projeto
![SCHEDULAGEM](/resources/airflow.png)

### Execução:
1. Fazer o download deste projeto a partir da branch `guilherme_santos`.
2. Executar os comandos em um interpretador bash de sua preferência:
    1. `export AIRFLOW_HOME=~/airflow`
    1. `export SPARK_PATH='/opt/spark-3.2.0-bin-hadoop3.2'`
    1. `export PYTHONPATH="${PYTHONPATH}:${pwd}"`
    1. `export SPARK_HOME=/opt/spark`
3. Executar o script `prepair_db.sh`, que recebe como parâmetro o nome do db a ser criado:
    1. `./prepair_db.sh data_driven`
        * Este script foi desenvolvido a fim de criar e excluir usuários e tabelas no banco de dados.
4. Em seguida, executar os seguintes comandos:
    1. `sudo pip instal -r requirements`
    1. `python3 setup.py install`
    1. `airflow db init`
    1. `airflow scheduler`
    * Caso não haja nenhum erro de isntalação, o serviço de schedulagem do airflow será inicializado.

3. Abrir um novo terminal e executar novamente o passo 2 descrito nesta seção, em seguida o comando:
    1. `airflow webserver --port 8008`.
    * Caso não haja nenhum erro de isntalação, o serviço de schedulagem do airflow será inicializado.

4. Abrir o navegador de sua preferência e digitar:
    1. `localhost:8080`.
    * A tela abaixo deverá ser exibida:
    ![AIRFLOWINIT](/resources/airflow_initt.png.png) 
    1. Abir a DAG e clicar no ícone de play, localizado no canto superior direito.
    1. Aguardar a execução do pipeline.
    1. OS dados estarão disponíveis para consulta nas tabelas:
        1. int_adult;
        2. tabelaa fact_adult.

### Conclusão:
1. A partir da tabela fato gerada, o analista responsável por analizar os dados disponibilizados através da engenharia presente neste projeto, poderá por exemplo:
    1. Classificar classes sociais de acordo com países.
    1. Capital de cada indivíduo segmentado pela idade.
    1. Verificar aumento ou diminuição do aumento de nível socioeconomico granularizado por idade, país, gênero.
    1. Identificar o padrão de renda de acordo com status civíl de cada invivíduo.
* Observação: estes insights, são apenas alguns que pode ser extraído das tabelas desenvolvidas.