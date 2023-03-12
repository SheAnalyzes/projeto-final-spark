from classes.class_dataframe import Dataframe
from classes.class_spark import PySpark
from classes.class_database import Database
from classes.class_fraudes import Fraudes
from dotenv import load_dotenv
from os import getenv

if __name__ == '__main__':
    # Carrega as variáveis de ambiente
    load_dotenv()

    # inicializar sessao spark
    path_jdbc_driver = getenv('PATH_JDBC_DRIVER')
    spark = PySpark(path_jdbc_driver).iniciar_sessao()

    # Criando dataframe para modelagem relacional
    csv_path = getenv('CSV_PATH')
    df_clientes = Dataframe(csv_path,'clients*').ler_csv(spark)
    df_transacoes = Dataframe(csv_path,'transaction*').ler_csv(spark)

    # Criando dataframe para projeto-sql
    df_transacoes_in = Dataframe(csv_path,'transaction-in*').ler_csv(spark)
    df_transacoes_out = Dataframe(csv_path,'transaction-out*').ler_csv(spark) 
    df_clientes = Dataframe(csv_path,'clients*').ler_csv(spark)

    # Criando df_fraudes para substituir o df_transacoes no modelo relacional
    df_fraudes = Fraudes(df_transacoes).criar_df_fraudes()
    # df_fraudes = Dataframe(df_fraudes).add_coluna(df_fraudes,'categoria')

    # Vendo dataframes
    print('Clientes')
    df_clientes.show()
    df_clientes.printSchema()
    print('Fraudes')
    df_fraudes.printSchema()
    df_fraudes.show()

    # Vendo dataframes
    print('T-I')
    df_transacoes_in.show()
    df_transacoes_in.printSchema()
    print('T-O')
    df_transacoes_out.printSchema()
    df_transacoes_out.show()
    
    
    # Escrevendo o DataFrame no SQL Server através do JDBC
    
    nome_tabela_fraudes = "TRANSACAO"
    nome_tabela_clientes = "CLIENTE"
    nome_tabela_transacoes_in = "TRANSACAO_IN"
    nome_tabela_transacoes_out = "TRANSACAO_OUT"

    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    database = getenv('DATABASE')
    jdbc_url = getenv('JDBC_URL')
    username = getenv('USERNAME')
    password = getenv('PASSWORD')

    conexao_database = Database(jdbc_url=jdbc_url, database=database, username=username, password=password, driver=driver)
    
    # Passando para o banco as tabelas usadas pelo projeto em SQL
    tabela_clientes = conexao_database.criar_tabela(df_clientes, nome_tabela_clientes)
    tabela_transacoes_in = conexao_database.criar_tabela(df_transacoes_in,nome_tabela_transacoes_in)
    tabela_transacoes_out = conexao_database.criar_tabela(df_transacoes_out,nome_tabela_transacoes_out)

    # Passando para o banco as tabelas usadas pelo projeto em SQL
    tabela_fraudes = conexao_database.criar_tabela(df_fraudes, nome_tabela_fraudes)

    # Finalizando sessão do Spark
    spark.stop()
    