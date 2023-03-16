# Accenture - Gama Academy - Mulheres em Tech - Data Engineer/Azure - Versão em Spark

Este é o repositório da versão do projeto em Spark.

Para retornar ao repositório geral, clique [aqui](https://github.com/SheAnalyzes/readme-repository)!

## Índice

- [Apresentação do problema](#apresentação-do-problema)
- [Tecnologias utilizadas](#tecnologias-utilizadas)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Instalando as bibliotecas](#instalando-as-bibliotecas)
- [Criação das funções que fazem a leitura do csv](#criação-das-funções-que-fazem-a-leitura-do-csv)
- [Criação da lógica de identificação das fraudes](#criação-da-lógica-de-identificação-de-fraudes)
- [Criação dos dataframes com a identificação das fraudes](#criação-dos-dataframes-com-a-identificação-das-fraudes)
- [Criação conexão com banco de dados](#criação-conexão-com-banco-de-dados)
- [Salvar os dados no banco de dados](#salvar-os-dados-no-banco-de-dados)
- [Instalação](#instalação)
- [Grupo - SheAnalyses](#grupo---sheanalyses)

## Apresentação do problema

Desenvolver uma aplicação em Python para carga de arquivos em um banco de dados SQL e gerar relatórios estatísticos visando a descoberta de fraudes em conta correntede cartão de crédito.

Você pode encontrar o link do desafio [aqui](https://docs.google.com/document/d/10fBZm7Sxm60FEIyNk4rqUE-pJLhXRxDi1grAATF7hVw/edit)!

## Tecnologias utilizadas

* Python;
* Spark;
* JDBC;
* SQL;
* Ferramentas do Azure.

## Estrutura do projeto

```
├── README.md
├── requirements.txt
├── image
│   └── README
│       ...
├── files
│   ...
├── jars
│   └── mssql-jdbc-12.2.0.jre8.jar
└── src
    ├── init.py
    ├── classes
    │   ├── __init__.py
    │   ├── csv_dataframe.py
    │   ├── database.py
    │   ├── fraud.py
    │   ├── screen.py
    │   ├── spark.py
    │   └── utils
    │       ├── dataframe_cleaner.py
    │       └── fraud_utils.py
    ├── __init__.py
    └── main.py
    
11 directories, 119 files
```

## Instalando as bibliotecas

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

8. Adicione o arquivo jar (que se encontra na pasta [jars](https://github.com/SheAnalyzes/projeto-final-spark/tree/main/jars)) na pasta '*spark-3.3.2-bin-hadoop3/jars*'
9. Digite em seu terminal o seguinte comando, lembrando de alterar o path até o arquivo .jar:
   ```
   export PYSPARK_SUBMIT_ARGS='--driver-class-path /mnt/c/Users/Mariana/spark-3.3.2-bin-hadoop3/jars/mssql-jdbc-12.2.0.jre8.jar pyspark-shell'
   ```
10. Criação do .gitignore
11. Salvando as versões usadas: `pip freeze > requirements.txt`

## Criação das funções que fazem a leitura do csv

Para isso, criamos a classe Dataframe. Essa classe tem o objetivo de proporcionar métodos capazes de ler arquivos CSVs, gerar dataframes e modificá-los.

## Criação da lógica de identificação de fraudes

Para isso, criamos a classe Fraudes. Essa classe procura identificar as fraudes presentes nos dataframes de transação bancária.

Ela utiliza algumas funções do pyspark:

1. Para utilizar a função partitionBy(), importamos:  `from pyspark.sql.window import Window`
2. Para utilizar as funções lag, unix_timestamp,when e lag, importamos: `pyspark.sql.functions import lag, unix_timestamp, when, col`

## Criação dos dataframes com a identificação das fraudes

Isso é feito na mesma classe de Fraudes.

## Criação conexão com banco de dados

1. Instalando o jdbc para conseguir estabelecer uma conexão com o banco de dados: `pip install pyjdbc`
2. No banco de dados, consiga a string de conexão.
3. Configure a string de conexão com login e senha.

## Salvar os dados no banco de dados

Isso é feito na classe Database. Essa classe procura realizar ações que manupulam as tabelas e dados de um database específico.

## Instalação

Para fazer a instalação das aplicações, siga os passos a seguir:

1. Clone o repositório específico em seu computador.
2. Abra o projeto no Microsoft Visual Studio Code ou outra IDE de sua preferência.
3. Certifique-se de ter o Python e o pip instalados em seu sistema.
4. Execute o comando a seguir: `pip install -r requirements.txt`
5. Execute a aplicação dando o comando '*python3 main.py*' em seu terminal.

## Notebook - Azure Synapse Analytics

O mesmo projeto foi reproduzido no Azure Synapse Analytics seguindo as etapas:

1. Criar Storage Account (Datalake).
2. Criar containers no Datalake. 
3. Fazer upload dos arquivos CSV para o container.
4. Criar workspace no Azure Synapse Analytics.
5. Criar spark pool.
6. Criar notebook spark e o configurar com o pool.
7. Executar notebook spark.

A princípio o pool será inicializado, demorando alguns minutos, mas em seguida a execução será rápida.
O acesso ao notebook desenvolvido pode ser encontrado no repositório.

## Grupo - SheAnalyses

![1678919788585](image/README/1678919788585.png)![1678922005355](image/README/1678922005355.png)

| Ana Paula Santos de Queiroz<br /><br />Linkedin: [/ana-paula-santos-de-queiroz-086807166](https://www.linkedin.com/in/ana-paula-santos-de-queiroz-086807166/)<br />Github: [/Queirozaps](https://github.com/Queirozaps) | ![1678913762981](image/README/1678913762981.png) |
| :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: | :--------------------------------------------: |
|  **Arianna Silveira Santos**<br />  <br />Linkedin: [/arianna-silveira-aa474514b](https://www.linkedin.com/in/arianna-silveira-aa474514b/)<br />Github: [/AriannaSilveira](https://github.com/AriannaSilveira)  | ![1678880182631](image/README/1678880182631.png) |
|                            **Carolina Gois**<br /><br />Linkedin: [/carolina-gois](https://www.linkedin.com/in/carolina-gois/)<br />Github: [/carolgois](https://github.com/carolgois)                            | ![1678915457372](image/README/1678915457372.png) |
|                   **Emilly Correa Santiago**<br /><br />Linkedin: [/emillysantiago23](https://www.linkedin.com/in/emillysantiago23/)<br />Github: [/emillysant](https://github.com/emillysant)                   | ![1678881122291](image/README/1678881122291.png) |
|                              **Mariana Freire**<br /><br />Linkedin: [/maricf](https://www.linkedin.com/in/maricf/)<br />Github: [/marianafreire](https://github.com/marianafreire)                              | ![1678915794465](image/README/1678915794465.png) |
|             **Priscila Assumpção Fernandes**<br /><br />Linkedin: [/priscila-af](https://www.linkedin.com/in/priscila-af/)<br />Github: [/priscilaassumpcao](https://github.com/priscilaassumpcao)             | ![1678916901964](image/README/1678916901964.png) |
|                    **Vivian Medina**<br /><br />Linkedin: [/vivian-medina-b7250961](https://www.linkedin.com/in/vivian-medina-b7250961/)<br />Github: [/medinavi](https://github.com/medinavi)                    | ![1678885040168](image/README/1678885040168.png) |
