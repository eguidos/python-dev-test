from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from src.raw import raw_adult
from src.integration import int_adult
from src.bussiness.dim import (dim_age, dim_class, dim_education,
                                dim_marital_status, dim_native_country, 
                                dim_occupation, dim_race, dim_relationship,
                                dim_sex, dim_workclass, dim_time)
from src.bussiness.fact import bs_fact_adult


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(0,0,0,0,0),
    "email": ['airflow@example.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retires": 1,
    "retray_delay": timedelta(minutes=1)

}

with DAG(dag_id="adult_dag", default_args=default_args) as dag:
    with TaskGroup("RAW") as raw:
        raw = PythonOperator(
        task_id='RAW_ADULT',
        python_callable=raw_adult.execute,
        dag=dag) 

    with TaskGroup("Integration") as integration:
        integration = PythonOperator(
        task_id='INTEGRATION_ADULT',
        python_callable=int_adult.execute,
        dag=dag)

    with TaskGroup("BS_DIMENSIONS") as dimensions:
        age = PythonOperator(
        task_id='BS_Dim_Age',
        python_callable=dim_age.execute,
        dag=dag)

        classes = PythonOperator(
        task_id='BS_Dim_class',
        python_callable=dim_class.execute,
        dag=dag)

        education = PythonOperator(
        task_id='BS_Dim_education',
        python_callable=dim_education.execute,
        dag=dag)

        marital_status = PythonOperator(
        task_id='BS_Marital_status',
        python_callable=dim_marital_status.execute,
        dag=dag)

        native_country = PythonOperator(
        task_id='BS_Dim_Native_country',
        python_callable=dim_native_country.execute,
        dag=dag)
    
        occupation = PythonOperator(
        task_id='BS_DIM_OCUPATION',
        python_callable=dim_occupation.execute,
        dag=dag)

        race = PythonOperator(
        task_id='BS_DIM_RACE',
        python_callable=dim_race.execute,
        dag=dag)

        relationship = PythonOperator(
        task_id='BS_DIM_RELATIONSHIP',
        python_callable=dim_relationship.execute,
        dag=dag)

        gender = PythonOperator(
        task_id='BS_DIM_SEX',
        python_callable=dim_sex.execute,
        dag=dag)

        workclass = PythonOperator(
        task_id='BS_DIM_WORKCLASS',
        python_callable=dim_workclass.execute,
        dag=dag)

        current_data = PythonOperator(
        task_id='BS_DIM_CURRENT_DATA',
        python_callable=dim_time.execute,
        dag=dag)

    with TaskGroup("BS_FACT") as fact:
        adult = PythonOperator(
        task_id="BS_FACT_ADULT",
        python_callable = bs_fact_adult.execute,
        dag=dag)

    raw >> integration >> dimensions >> fact