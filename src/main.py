from classes.dataframe import CsvDataframe
from classes.spark import PySpark
from classes.database import Database
from classes.fraud import Fraud
from dotenv import load_dotenv
from os import getenv


if __name__ == '__main__':
    
    # Carrega as variáveis de ambiente
    load_dotenv()

    # inicializar sessao spark
    path_jdbc_driver = getenv('PATH_JDBC_DRIVER')
    spark = PySpark(path_jdbc_driver).start_session()

    # Criando dataframe para modelagem relacional
    csv_path = getenv('CSV_PATH')
    df_clientes = CsvDataframe(csv_path,'clients*').read_csv(spark)
    df_transacoes = CsvDataframe(csv_path,'transaction*').read_csv(spark)

    # Criando dataframe para projeto-sql
    df_transacoes_in = CsvDataframe(csv_path,'transaction-in*').read_csv(spark)
    df_transacoes_out = CsvDataframe(csv_path,'transaction-out*').read_csv(spark) 

    # Criando df_fraudes para substituir o df_transacoes no modelo relacional
    column_name = 'categoria'
    condition_column = 'valor'
    df_fraudes = Fraud(df_transacoes).create_fraud_df()

    df_fraudes.printSchema()
    df_fraudes.show()
    df_clientes.printSchema()
    df_clientes.show()

    # Escrevendo o DataFrame no SQL Server através do JDBC
    
    fraud_table_name = "TRANSACAO"
    client_table_name = "CLIENTE"
    transaction_in_table_name = "TRANSACAO_IN"
    transaction_out_table_name = "TRANSACAO_OUT"

    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    database = getenv('DATABASE')
    jdbc_url = getenv('JDBC_URL')
    username = getenv('USERNAME')
    password = getenv('PASSWORD')

    conexao_database = Database(jdbc_url=jdbc_url, database=database, username=username, password=password, driver=driver)
    
    # Passando para o banco as tabelas usadas pelo projeto em SQL
    tabela_clientes = conexao_database.create_table(df_clientes, client_table_name)
    tabela_transacoes_in = conexao_database.create_table(df_transacoes_in,transaction_in_table_name)
    tabela_transacoes_out = conexao_database.create_table(df_transacoes_out, transaction_out_table_name)

    # Passando para o banco as tabelas usadas pelo projeto em SQL
    tabela_fraudes = conexao_database.create_table(df_fraudes, fraud_table_name)

    # Finalizando sessão do Spark
    spark.stop()
    