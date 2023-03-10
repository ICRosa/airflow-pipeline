# Airflow-pipeline
Pipeline de airflow criada com microserviços

Aqui temos arquivos referentes a construção de um cluster kubernetes contendo o orquestrador airflow e os bancos de dados postgresql e mysql. Dentro do airflow temos uma dag de ETL simples entre os dois bancos e outra de coleta, conversão, transformação e analise dos dados.

# Dentre as principais ferramentas e tecnologias utilizadas nesse projeto, temos:

   - Airflow
   - Amazon S3
   - Amazon IAM
   - Git-Sync
   - Helm
   - Kubenetes
   - MySQL
   - Python (e suas bibliotecas)
   - Postgresql


---

## *Passo a passo para remontar a estrutura*


 - Criar repositorio GitHub para Dags
 - Iniciar cluster Kubernetes - *Helm intalls.txt contém detalhes*
 - Configurar bucket S3 com arquivos
 - Configurar usuario aws com chave de acesso ReadWrite para S3
 - Criar conecções no airflow e substituir valores nescessarios nas dags
 - Rodar dags e ver funcionando
