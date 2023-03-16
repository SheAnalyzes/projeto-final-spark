from classes.csv_dataframe import CsvDataframe
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

    # Define JDBC connection variables & env variables
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    path_jdbc_driver = getenv('PATH_JDBC_DRIVER')
    database = getenv('DATABASE')
    jdbc_url = getenv('JDBC_URL')
    username = getenv('USERNAME')
    password = getenv('PASSWORD')
    csv_path = getenv('CSV_PATH')

    # Create objects
    screen = Screen()       
    spark = PySpark(path_jdbc_driver).start_session()
    database_connection = Database(jdbc_url=jdbc_url, database=database, username=username, password=password, driver=driver)
    
    screen.clean()

    stage_3 = False
    stage_4 = False

    while True:
    
        # Menu
        print("-"*100)
        print("Bem vinde ao processo de ETL do She Analyses!\nPara dar continuidade, note que as opções que temos são sequenciais:")
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
            print("Descrição: Desenvolver uma aplicação em Python para carga de arquivos em um banco de dados SQL e gerar relatórios estatísticos visando a descoberta de fraudes em conta corrente.\nDada a descrição, o Script de migração do She Analyzes busca tratar os dados dos arquivos CSV fornecidos, bem como identificar as fraudes. Por fim, migramos criamos tabelas e as migramos para o nosso Database.\n")
            screen.wait(3)

        elif option == 2:
            screen.clean()
            
            print("Lendo os arquivos CSV...")

            # Create dataframes based in our relational model
            clients_df_obj = CsvDataframe(csv_path,'clients*')
            clients_df_obj.read_csv(spark)
            clients_raw_df = clients_df_obj.df

            transactions_df_obj = CsvDataframe(csv_path,'transaction*')
            transactions_df_obj.read_csv(spark)
            
            # Create dataframe based in raw to projeto-final-sql
            transactions_in_obj = CsvDataframe(csv_path,'transaction-in*')
            transactions_in_obj.read_csv(spark)
            transactions_in_df = transactions_in_obj.df

            transactions_out_obj = CsvDataframe(csv_path,'transaction-out*')
            transactions_out_obj.read_csv(spark)
            transactions_out_df = transactions_out_obj.df

            raw_df_dict = {
                'Dataframe de Transações de Entrada': transactions_in_df, 
                'Dataframe de Transações de Saída':transactions_out_df, 
                'Dataframe de Clientes': clients_raw_df
                }
            screen.show_df(raw_df_dict)
            stage_3 = True

        elif option == 3:
            screen.clean()

            if stage_3 == True: 
                print("Interceptando operações fraudulentas...")

                # Clean dataframes
                clients_df = clients_df_obj.clean_data()
                transactions_df_obj.rename_column('data','data_transacao')
                transactions_df = transactions_df_obj.clean_data()

                #Create df_fraudes to replace the df_transacoes based in our relational model
                column_name = 'categoria'
                condition_column = 'valor'
                frauds_df = Fraud(transactions_df).create_fraud_df()

                clean_df_dict = {
                    'Dataframe de Transações': frauds_df, 
                    'Dataframe de Clientes': clients_df
                    }
                screen.show_df(clean_df_dict)
                final_stage = True
            
            else:
                print("Parece que você está tentando interceptar as operações fraudulentas sem ter carregado os CSVs.\n")

        elif option == 4:
            screen.clean()

            if final_stage == True:
                print("Gerando arquivos CSV baseados nos dados tratados...")
                transactions_df_obj.save_csv_file('reports-transaction-frauds')
                clients_df_obj.save_csv_file('reports-clients')
                
                print("Arquivos CSV gerados!")
                stage_5 = True
            else:
                print("Parece que você está tentando carregar os CSVs sem ter cumprido as etapas 2 e 3!\n")
            screen.wait(3)

        elif option == 5:
            screen.clean()

            if final_stage == True:
                print("Aguarde...\nSalvando operações no Banco...")

                # Create table in database and populate it based in the dataframe
                client_table = database_connection.create_table(clients_df, 'clientes_teste')
                transaction_table = database_connection.create_table(frauds_df, 'transacoes_teste')
                screen.wait(3)
            else:
                print("Parece que você está tentando salvar as tabelas no Banco sem ter cumprido as etapas 2 e 3!\n")

        elif option == 6:
            spark.stop()
            exit()
        
        else:
            print('Opção invalida! Por favor, digite um número de 1 a 6.')
            screen.wait(3)
            screen.clean()