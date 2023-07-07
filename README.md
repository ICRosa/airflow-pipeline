# Airflow Pipeline

Este é um projeto de pipeline de dados utilizando o Apache Airflow, com o objetivo de realizar o processo de ETL (Extract, Transform, Load) entre bancos de dados MySQL e PostgreSQL. Além disso, também inclui uma pipeline automatizada de limpeza e criação de modelo para a equipe de análise.

## Tecnologias e Ferramentas Utilizadas

- Apache Airflow
- Amazon S3
- Amazon IAM
- Git-Sync
- Helm
- Kubernetes
- MySQL
- PostgreSQL
- Python (e suas bibliotecas)

## Estrutura do Projeto

O projeto consiste em um cluster Kubernetes contendo os seguintes componentes:

- Orquestrador Apache Airflow
- Bancos de dados MySQL e PostgreSQL

Dentro do Apache Airflow, temos a implementação de duas DAGs principais:

1. **DAG de ETL**:
   - Essa DAG realiza o processo de ETL entre o banco de dados MySQL e o banco de dados PostgreSQL.
   - Ela extrai dados específicos do MySQL, transforma-os e carrega-os no PostgreSQL.
   - A DAG possui três etapas principais: extração, transformação e carga dos dados.
   - Cada etapa é representada por um operador Python no Apache Airflow.

2. **DAG pipeline automatizada de limpeza e criação de modelo**:
   - Essa DAG é responsável por realizar a limpeza de uma base de dados e criar modelos para a equipe de análise.
   - Ela realiza uma série de transformações nos dados, como remoção de duplicatas, preenchimento de valores ausentes e codificação de variáveis categóricas.
   - A DAG também inclui a etapa de treinamento de modelos de regressão usando diferentes algoritmos.
   - Os modelos treinados são armazenados em disco para uso posterior.

**Observação**: O código base foi adquirido no curso de Airflow da Stack Academy.

## Pré-requisitos

Antes de executar o projeto, é necessário realizar as seguintes etapas:

1. Criar um repositório no GitHub para armazenar as DAGs.
2. Iniciar um cluster Kubernetes, seguindo as instruções contidas no arquivo `Helm installs.txt`.
3. Configurar um bucket no Amazon S3 para armazenar arquivos necessários.
4. Configurar um usuário IAM na AWS com permissões de leitura e gravação no S3.
5. Criar conexões no Apache Airflow para os bancos de dados MySQL e PostgreSQL e substituir os valores necessários nas DAGs.
6. Executar as DAGs no Apache Airflow e verificar o funcionamento correto.

## Docker Build

Para construir uma imagem Docker com as dependências necessárias, utilize o seguinte Dockerfile:

```dockerfile
FROM apache/airflow
RUN pip install --no-cache-dir scikit-learn 
RUN pip install --no-cache-dir apache-airflow-providers-papermill 
RUN pip install --no-cache-dir papermill[all]
RUN pip install --no-cache-dir pandas numpy matplotlib seaborn
RUN pip install --no-cache-dir jupyter
RUN pip install --no-cache-dir pymysql psycopg2-binary
RUN pip install --no-cache-dir apache-airflow-providers-amazon
RUN pip install --no-cache-dir apache-airflow[amazon]
```

Para construir a imagem, execute o seguinte comando no diretório onde se encontra o arquivo Dockerfile:

```shell
docker build -t <nome_da_imagem> .
```

Certifique-se de substituir `<nome_da_imagem>` pelo nome desejado para a imagem.

Após a construção da imagem, você pode prosseguir com os passos descritos anteriormente para fazer o push da imagem para sua conta no DockerHub.

## Arquivos Adicionais (opcionais)

A seguir, alguns comandos e exemplos adicionais que podem ser úteis:

- Criação de um 'kubernetes secret' a partir de uma chave SSH para sincronização de repositório Git privado:

```shell
kubectl create secret generic <nome_do_secret> --from-file=gitSshKey=<chave_ssh>
```

- Exemplo de criação de uma string de conexão (no caso, PostgreSQL) codificada em base64:

```shell
echo -n "Server=<ip>; Port=5432; UserId=postgres; Password=admin;" | base64
```

- Criação de um 'kubernetes secret' a partir de uma string de conexão:

```shell
kubectl create secret generic <nome_do_secret> --from-literal=connection=<string_de_conexao>
```

- Geração de uma 'fernet key' usada para segurança de criptografia no Apache Airflow:

```shell
python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)"
```

- Exemplo de redirecionamento de porta (port forward) para conectar-se a um serviço fora do cluster:

```shell
kubectl port-forward svc/airflow-pl-webserver 8080:8080
```

## Configuração do Apache Airflow (values.yaml)

O arquivo `values.yaml` contém a configuração do Apache Airflow. Nele, é possível definir algumas opções importantes:

- Chave Fernet (`fernetkey`): Essa chave é necessária para a segurança de criptografia no Apache Airflow. Substitua `<fernet Key>` pela chave desejada.
- Configuração do Git Sync (`gitSync`): Nessa seção, é possível habilitar a sincronização com um repositório Git e especificar o caminho e as informações do repositório.
- Configuração do Ingress (`ingress`): Aqui, é possível configurar um ingress para permitir acesso externo ao Apache Airflow sem a necessidade de redirecionamento de porta.
- Configuração do Banco de Dados PostgreSQL (opcional): Caso deseje utilizar um banco de dados PostgreSQL externo, é possível descomentar a seção `postgresql` e fornecer as informações de conexão.

## Conclusão

Esse projeto de pipeline de dados utilizando o Apache Airflow demonstra a construção de uma DAG de ETL entre bancos de dados MySQL e PostgreSQL, além de uma pipeline automatizada de limpeza e criação de modelo para a equipe de análise. Ele utiliza tecnologias como Helm, Kubernetes, Amazon S3, Git-Sync e Docker para criar uma estrutura eficiente e escalável.

Para executar o projeto, siga os passos descritos acima e adapte-os de acordo com as suas necessidades. Certifique-se de verificar todas as dependências e configurações antes de executar as DAGs no Apache Airflow.
