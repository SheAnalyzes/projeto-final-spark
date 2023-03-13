# Aplicação em Python para detecção de fraudes bancárias

Este é um programa escrito em Python. O programa lê arquivos de entrada, carrega os dados em um banco de dados SQL e a partir daí foi possível gerar relatórios, utilizando a plataforma PowerBI, para identificar transações suspeitas.

## Índice

- [Visão geral](#visão-geral)
- [Funcionalidades](#funcionalidades)
- [Arquivos de entrada](#arquivos-de-entrada)
- [Banco de dados](#banco-de-dados)
- [Tecnologias utilizadas](#tecnologias-utilizadas)
- [Versão em Pandas](#versão-em-pandas)
- [Versão em Spark](#versão-em-spark)
- [Instalação](#instalação)
- [Modelagem de entidades e relacional](#modelagem-de-entidades-e-relacional)
- [Relatórios do Power BI](#relatórios-em-power-bi)
- [Grupo - SheAnalyses](#grupo---sheanalyses)

## Visão geral

Seu objetivo inicial é analisar os arquivos dados criando uma base de dados relacional para fazer a carga e depois analisá-la. A movimentação bancária fraudulenta será aquela que possuir movimentações abaixo de 2 minutos de espaçamento entre as transações.

## Funcionalidades

* Leitura dos arquivos csv.
* Identificação das operações fraudulentas.
* Carregamento dos dados em um banco de dados.
* Análise dos dados obtidos.

## Arquivos de entrada

O programa lê três categorias de arquivos de entrada:

* clients: informações dos clientes, incluindo nome, endereço de e-mail, data de cadastro e telefone.

* transactions-in: informações de transações de entrada para as contas dos clientes, incluindo também valor e data.
* transactions-out: informações de transações de saída para as contas dos clientes, incluindo também valor e data.

Os arquivos de entrada podem ser encontrados nos repositórios. Você pode acessá-los [aqui](https://github.com/SheAnalyzes/projeto-final-pandas/tree/master/arquivos_carga_csv).

## Banco de dados

O programa usa um banco de dados SQL para armazenar os dados dos clientes e transações.

O banco de dados foi criado através das ferramentas da Azure.

## Tecnologias utilizadas

Ao analisar a situação do problema, percebe-se que a quantidade de dados a ser processada não é tão grande.

Por isso, o grupo entendeu que a melhor alternativa a ser utilizada seria o Pandas. 

Porém, também fizemos uma versão em Spark, para fins de aprendizagem e poder demonstrar o nosso conhecimento na ferramenta.

As tecnologias utilizadas em nossas aplicações foram:

* Python;
* Pandas;
* Spark;
* SQL;
* Ferramentas do Azure.

## Versão em Pandas

#### 1 - Criando um ambiente virtual no Windows:

1. Na pasta do projeto, digite no terminal: `python -m venv venv`
2. Ativando a venv: `venv\Scripts\activate`
3. Verificando se está com a ultima versão do pip: `python -m pip install --upgrade pip`

#### 2 - Instalando as bibliotecas:

1. Instalação do Pandas: `pip install pandas`
2. Criação do gitignore e adicionando a venv nele
3. Salvando as versões usadas: `pip freeze > requirements.txt`

#### 3 - Criação das funções que fazem a leitura do csv

#### 4 - Criação da logica para gerar o relatório de fraudes

#### 5 - Criação dos relatórios em csv

#### 6 - Criar conexão com banco de dados

1. Instalando o pyodbc para conseguir estabelecer uma conexão com o banco de dados: `pip install pyodbc`
2. Para instalação do driver ODBC 18, siga as instruções dadas no site da Microsoft (acesse [aqui]([https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15))).
3. Configure a conexão com login e senha.

#### 7 - Salvar os dados no banco de dados.

## Versão em Spark

#### 1 - Estrutura do projeto:

```
├── README.md
├── estrutura.txt
├── files
│   ...
├── jars
│   └── mssql-jdbc-12.2.0.jre8.jar
├── requirements.txt
└── src
    ├── init.py
    ├── classes
    │   ├── init.py
    │   ├── class_database.py
    │   ├── class_dataframe.py
    │   ├── class_fraudes.py
    │   └── class_spark.py
    └── main.py
4 directories, 87 files
```


#### 1 - Instalando as bibliotecas:

1. Verifique se você possui o Java instalado. Para isso, no terminal digite: `java -version`
2. Caso não possua, para instalar, insira o comando: `sudo apt install openjdk-11-jre-headless`
3. Verifique se possui o Python instalado: `python3 --version`
4. Caso não possua, insira: `sudo apt install python3.8`
5. Instalação do Pandas: `pip install pandas`
6. Instalação do Pyspark:
7. Instalação do Spark:

```
pip install pyspark
wget -q https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz  
tar -xvzf spark-3.3.2-bin-hadoop3.tgz
pip install -q findspark
```

8. Criação do .gitignore
9. Salvando as versões usadas: `pip freeze > requirements.txt`

#### 3 - Criação das funções que fazem a leitura do csv:

Para isso, criamos a classe Dataframe. Essa classe tem o objetivo de proporcionar métodos capazes de ler arquivos CSVs, gerar dataframes e modificá-los.

#### 4 - Criação da lógica de identificação de fraudes:

Para isso, criamos a classe Fraudes. Essa classe procura identificar as fraudes presentes nos dataframes de transação bancária.

Ela utiliza algumas funções do pyspark:

1. Para utilizar a função partitionBy(), importamos:  `from pyspark.sql.window import Window`
2. Para utilizar as funções lag, unix_timestamp,when e lag, importamos: `pyspark.sql.functions import lag, unix_timestamp, when, col`

#### 5 - Criação dos dataframes com a identificação das fraudes.

Isso é feito na mesma classe de Fraudes.

#### 6 - Criar conexão com banco de dados

1. Instalando o jdbc para conseguir estabelecer uma conexão com o banco de dados: `pip install pyjdbc`
2. No banco de dados, consiga a string de conexão.
3. Configure a string de conexão com login e senha.

#### 7 - Salvar os dados no banco de dados.

Isso é feito na classe Database. Essa classe procura realizar ações que manupulam as tabelas e dados de um database específico.

## Instalação

Para fazer a instalação das aplicações, siga os passos a seguir:

1. Clone o repositório específico em seu computador.
2. Abra o projeto no Microsoft Visual Studio Code ou outra IDE de sua preferência.
3. Certifique-se de ter o Python e o pip instalados em seu sistema.
4. Execute o comando a seguir: `pip install -r requirements.txt`
5. Execute a aplicação dando o comando '*python3 main.py*' em seu terminal.

## Modelagem de entidades e relacional


## Relatórios em Power BI


## Grupo - SheAnalyses
