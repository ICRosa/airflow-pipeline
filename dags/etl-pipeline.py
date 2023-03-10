#A base dessa dage foi tirada do curso Data Pipelines com Apache Airflow da Stack tecnologias

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
#from airflow.operators.#_operator import #Operator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import pandas as pd
import sqlalchemy
import logging




path_temp_csv = "/tmp/dataset.csv"
##_failed = "felipesf05@gmail.com"

dag = DAG(
    dag_id="elt-pipeline",
    description="Pipeline para o processo de ETL, MySQL para Postgresql.",
    start_date=days_ago(2),
    schedule_interval=None,
)

##BELOW OPTION INSTALL PYTHON DEPENDENCIES WITH PIP (NO NEEDED IF THEY ARE ALREADY BUILDED IN AIRFLOW CONTAINER)
'''
##INSTALA PACOTES PYTHON COM PIP (NÃO É NECESSÁRIO SE JÁ ESTIVEREM CONSTRUÍDOS NO CONTÊINER AIRFLOW)

@task(task_id="_install")
def _install():
    import os
    import pip 
    # os.system('pip install' + "PyMySQL" + "psycopg2" + '-y')
    pip.main(['install', "pymysql", "psycopg2-binary"])

import_task = _install()
'''
 
def _extract():
    #conectando a base de dados MySQL.
    engine_mysql_oltp = sqlalchemy.create_engine("mysql+pymysql://<USER>:<SENHA>@<ip>:3306/<DATABASE>")
    
    #selecionando os dados.
    dataset_df = pd.read_sql_query(r"""
                        SELECT   emp.emp_no
                        , emp.first_name
                        , emp.last_name
                        , sal.salary
                        , titles.title 
                        FROM employees emp 
                        INNER JOIN (SELECT emp_no, MAX(salary) as salary 
                                    FROM salaries GROUP BY emp_no) 
                        sal ON sal.emp_no = emp.emp_no 
                        INNER JOIN titles 
                        ON titles.emp_no = emp.emp_no
                        LIMIT 1000"""
                        ,engine_mysql_oltp
    )
    #exportando os dados para a área de stage.
    dataset_df.to_csv(
        path_temp_csv,
        index=False
    )

def _transform():
    
    dataset_df = pd.read_csv(path_temp_csv)

    #transformando os dados dos atributos.
    dataset_df["name"] = dataset_df["first_name"]+" "+dataset_df["last_name"]
    
    dataset_df.drop([    "emp_no"
                        ,"first_name"
                        ,"last_name"
                    ]
                    ,axis=1
                    ,inplace=True)
    
    #persistindo o dataset no arquivo temporario.
    dataset_df.to_csv(
        path_temp_csv,
        index=False
    )

def _load():
    #conectando com o banco de dados postgresql'

    conn_str = "postgresql+psycopg2://<USER>:<SENHA>@<IP>:5432/<DATABASE>"
    engine_postgresql_olap = sqlalchemy.create_engine(conn_str)
    
    #lendo os dados a partir dos arquivos csv.
    dataset_df = pd.read_csv(path_temp_csv)
    
    logging.basicConfig()
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    #carregando os dados no banco de dados.
    dataset_df.to_sql("employees_dataset", con=engine_postgresql_olap, if_exists="replace",index=False) 

    '''
    ##TAMBÉM FUNCIONA COM UMA CONECÇÃO POSTGRESQL-AIRFLOW CHAMADA "psqlconn"

    from airflow.hooks.postgres_hook import PostgresHook
    p_h = PostgresHook("psqlconn")
    engine_postgresql_olap = p_h.get_sqlalchemy_engine()
    dataset_df = pd.read_csv(path_temp_csv)
    dataset_df.to_sql("employees_dataset2", con=engine_postgresql_olap, if_exists="replace",index=False)
    '''


extract_task = PythonOperator(
    task_id="Extract_Dataset", 
    python_callable=_extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id="Transform_Dataset", 
    python_callable=_transform, 
    dag=dag
)

load_task = PythonOperator(
    task_id="Load_Dataset",
    python_callable=_load,
    dag=dag
)

clean_task = BashOperator(
    task_id="Clean",
    bash_command="rm -f /tmp/*.csv",
    dag=dag
)



extract_task >> transform_task >> load_task >> clean_task 
