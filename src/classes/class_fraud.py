from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp, when, col
from classes.class_dataframe import Dataframe

class Fraud(Dataframe):
    '''The Fraud class aims to provide different methods capable of detecting and analyzing potential frauds in banking transaction dataframes.'''

    def __init__ (self, df_transactions):
        self.df_transactions = df_transactions

    def _identify_frauds(self):
        '''This method identifies frauds and creates an extra column in the original dataframe.'''

        # Partitioning the transactions df (in and out) by the "cliente_id" column and ordering by "data".
        windowSpec  = Window.partitionBy("cliente_id").orderBy("data")

        # Using the lag function to create a new column (based on the "data" column) with the previous row information
        df = self.df_transactions.withColumn("data_anterior",lag("data",1).over(windowSpec))

        # Transforming dates ("data" and "data_anterior") to seconds
        df = df\
            .withColumn("data_seg", unix_timestamp("data", "yyyy-MM-dd HH:mm:ss Z"))\
            .withColumn("data_anterior_seg", unix_timestamp("data_anterior", "yyyy-MM-dd HH:mm:ss Z"))

        # Calculating the difference between the previous rows
        df = df.withColumn("diff", col("data_seg") - col("data_anterior_seg"))

        # Creating a column that determines if there was fraud
        self.df = df.withColumn("suspeita_de_fraude", when(col("diff") < 120, 'valida').otherwise('invalida'))

        return self.df

    def create_fraud_df(self):
        '''This method generates dataframes that include columns signaling the presence or absence of fraud suspicions.'''
        column_name = 'categoria'
        condition_column = 'valor'
        df_col_frauds = self._identify_frauds()
        self.fraud_df = super().join_csv(df_col_frauds, self.df_transactions)
        self.fraud_df = super().add_column(column_name, condition_column, self.fraud_df)
        return self.fraud_df
    
