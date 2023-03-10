FROM apache/airflow
RUN pip install --no-cache-dir scikit-learn 
RUN pip install --no-cache-dir apache-airflow-providers-papermill 
RUN pip install --no-cache-dir papermill[all]
RUN pip install --no-cache-dir pandas numpy matplotlib seaborn
RUN pip install --no-cache-dir jupyter
RUN pip install --no-cache-dir pymysql psycopg2-binary
RUN pip install --no-cache-dir apache-airflow-providers-amazon
RUN pip install --no-cache-dir apache-airflow[amazon]