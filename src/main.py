from classes.dataframe import CsvDataframe
from classes.spark import PySpark
from classes.database import Database
from classes.fraud import Fraud
from classes.screen import Screen
from dotenv import load_dotenv
from os import getenv

if __name__ == '__main__':
     
    #Configure system

    # Load enviroment variables
    load_dotenv()

    # Define table names
    fraud_table_name = "TRANSACAO"
    client_table_name = "CLIENTE"
    transaction_in_table_name = "TRANSACAO_IN"
    transaction_out_table_name = "TRANSACAO_OUT"

    # Define JDBC conection variables
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    path_jdbc_driver = getenv('PATH_JDBC_DRIVER')
    database = getenv('DATABASE')
    jdbc_url = getenv('JDBC_URL')
    username = getenv('USERNAME')
    password = getenv('PASSWORD')

    # Create objects
    screen = Screen()       
    spark = PySpark(path_jdbc_driver).start_session()
    conexao_database = Database(jdbc_url=jdbc_url, database=database, username=username, password=password, driver=driver)
    
    screen.clean()

    stage_3 = False
    stage_4 = False
    stage_5 = False

    while True:
    
        # Menu
        print("\nBem vinde ao processo de ETL do She Analyses!\nPara dar continuidade, note que as opções que temos são sequenciais:")
        print("1 - Informações sobre o Desafio.")
        print("2 - Ler os arquivos CSV.")
        print("3 - Interceptar operações fraudulentas.")
        print("4 - Gerar arquivos CSV.")
        print("5 - Salvar dados no Database - SQL Server.")
        print("6 - Sair")
        print("-"*100)
        
        option = int(input("Digite o número da opção: "))

        if option == 1:
            screen.clean()
            print("1 - Informações sobre o Desafio.")
            print("Descrição: Desenvolver uma aplicação em Python para carga de arquivos em um banco de dados SQL e\
gerar relatórios estatísticos visando a descoberta de fraudes em conta corrente.\n\
Dada a descrição, o Script de migração do She Analyzes busca tratar os dados dos arquivos CSV fornecidos,\
bem como identificar as fraudes. Por fim, migramos criamos tabelas e as migramos para o nosso Database.\n")
            screen.wait(3)
        
        elif option == 2:
            screen.clean()
            
            print("Lendo os arquivos CSV...")

            # Create dataframes based in our relational model
            csv_path = getenv('CSV_PATH')
            clients_df = CsvDataframe(csv_path,'clients*').read_csv(spark)
            transactions_df = CsvDataframe(csv_path,'transaction*').read_csv(spark)

            # Create dataframe based in raw to projeto-final-sql
            transactions_in_df = CsvDataframe(csv_path,'transaction-in*').read_csv(spark)
            transactions_out_df = CsvDataframe(csv_path,'transaction-out*').read_csv(spark)

            raw_df_dict = {
                'Dataframe de Transições de Entrada': transactions_in_df, 
                'Dataframe de Transições de Saída':transactions_out_df, 
                'Dataframe de Clientes': clients_df
                }
                
            screen.show_df(raw_df_dict)
            stage_3 = True


        elif option == 3:
            screen.clean()

            if stage_3 == True: 
                print("Interceptando operações fraudulentas...")

                # Clean dataframes
                clients_df = CsvDataframe(csv_path,'clients*').clean_data(clients_df)
                transactions_df = CsvDataframe(csv_path,'transaction*').clean_data(transactions_df)

                # Create df_fraudes to replace the df_transacoes based in our relational model
                column_name = 'categoria'
                condition_column = 'valor'
                frauds_df = Fraud(transactions_df).create_fraud_df()

                clean_df_dict = {
                    'Dataframe de Transações': frauds_df, 
                    'Dataframe de Clientes': clients_df
                    }

                screen.show_df(clean_df_dict)
                stage_4 = True
            
            else:
                print("Parece que você está tentando interceptar as operações fraudulentas sem ter carregado os CSVs.\n")

        elif option == 4:
            screen.clean()

            if stage_4 == True:
                print("Gerando arquivos CSV baseados nos arquivos de carga...")
                transactions_in_df.coalesce(1).write.option("header", "true").csv("../reports/transaction-in.csv")
                #transactions_in_df.write.format("csv").option("header", "true").save("/mnt/c/Users/Mariana/Desktop/Projeto-Final/Spark/reports/" + 'transactions_in.csv')
                #dataframe_teste1 = CsvDataframe(csv_path,'transaction-in*').save_csv_file(transactions_in_df, 'transactions_in.csv')
                #dataframe_teste2 = CsvDataframe(csv_path,'transaction-out*').save_csv_file(transactions_out_df, 'transactions_out.csv')

                print("Gerando arquivos CSV baseados nos dados tratados...")
                #CsvDataframe.save_csv_file(frauds_df, 'transaction_frauds')
                #CsvDataframe.save_csv_file(clients_df, 'clients')
                
                print("Arquivos CSV gerados!")
                stage_5 = True
            else:
                print("Parece que você está tentando carregar os CSVs sem ter cumprido as etapas anteriores.\n")
            screen.wait(3)

        elif option == 5:
            screen.clean()

            if stage_5 == True:
                print("Aguarde...\nSalvando operações no Banco...")

                # Create table in database and populate it based in the dataframe
                transaction_in_table = conexao_database.create_table(transactions_in_df,'TRASACAO_IN')
                transaction_out_table = conexao_database.create_table(transactions_out_df, 'TRANSACAO_OUT')
                client_table = conexao_database.create_table(clients_df, 'CLIENTE')
                transaction_table = conexao_database.create_table(frauds_df, 'TRANSACAO')
                screen.wait(3)
            else:
                print("Parece que você está tentando salvar as tabelas no Banco sem ter cumprido as etapas anteriores.\n")

        elif option == 6:
            PySpark.end_session(spark)
            exit()
        
        else:
            print('Opção invalida! Por favor, digite um número de 1 a 6.')
            screen.wait(3)
            screen.clean()