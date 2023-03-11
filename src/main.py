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

    # Criando dataframe
    csv_path = getenv('CSV_PATH')

    df_transacoes = Dataframe(csv_path,'transaction*').criar(spark)    
    df_clientes = Dataframe(csv_path,'clients*').criar(spark)
    df_teste = Dataframe('/mnt/c/Users/Mariana/Desktop/','teste*').criar(spark)

    # Encontrando fraudes e criando df fraudes
    df_fraudes = Fraudes(df_transacoes).criar_df_fraudes()
    df_teste_fraudes = Fraudes(df_teste).criar_df_fraudes()
    
    # Vendo dataframes
    print('Transacoes')
    df_transacoes.show()
    print('Clientes')
    df_clientes.show()
    print('Fraudes')
    df_fraudes.show()
    
    
    # Escrevendo o DataFrame no SQL Server através do JDBC
    
    nome_tabela_fraudes = "Fraudes"
    nome_tabela_clientes = "Clientes"
    nome_tabela_transacoes = "Transacoes"
    
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    database = getenv('DATABASE')
    jdbc_url = getenv('JDBC_URL')
    username = getenv('USERNAME')
    password = getenv('PASSWORD')

    conexao_database = Database(jdbc_url=jdbc_url, database=database, username=username, password=password, driver=driver)
    
    tabela_transacoes = conexao_database.criar_tabela(df_transacoes, nome_tabela_transacoes)
    tabela_clientes = conexao_database.criar_tabela(df_clientes, nome_tabela_clientes)
    tabela_fraudes = conexao_database.criar_tabela(df_fraudes, nome_tabela_fraudes)
    tabela_teste = conexao_database.criar_tabela(df_teste_fraudes,'Teste')
    tabela_teste.show()
    # Finalizando sessão do Spark
    spark.stop()