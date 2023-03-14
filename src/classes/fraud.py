from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp, when, col
from classes.utils.fraud_utils import FraudUtils

class Fraud():
    '''The Fraud class aims to provide different methods capable of detecting and analyzing potential frauds in banking transaction dataframes.'''
    frauds_df = None

    def __init__ (self, df_transactions):
        self.df_transactions = df_transactions

    def _identify_frauds(self):
        '''This method identifies frauds and creates an extra column in the original dataframe.'''

        # Partitioning the transactions df (in and out) by the "cliente_id" column and ordering by "data".
        windowSpec  = Window.partitionBy("cliente_id").orderBy("data_transacao")

        # Using the lag function to create a new column (based on the "data" column) with the previous row information
        self.fraud_df = self.df_transactions.withColumn("data_anterior",lag("data_transacao",1).over(windowSpec))

        # Transforming dates ("data" and "data_anterior") to seconds
        self.fraud_df = self.fraud_df\
            .withColumn("data_seg", unix_timestamp("data_transacao", "yyyy-MM-dd HH:mm:ss Z"))\
            .withColumn("data_anterior_seg", unix_timestamp("data_anterior", "yyyy-MM-dd HH:mm:ss Z"))

        # Calculating the difference between the previous rows
        self.fraud_df = self.fraud_df.withColumn("diff", col("data_seg") - col("data_anterior_seg"))

        # Creating a column that determines if there was fraud
        self.fraud_df = self.fraud_df.withColumn("fraude", when(col("diff") < 120, '1').otherwise('0'))

    def create_fraud_df(self):
        '''This method generates dataframes that include columns signaling the presence or absence of fraud suspicions.'''

        self._identify_frauds()
        column_name = 'tipo'
        condition_column = 'valor'

        self.fraud_utils_obj = FraudUtils(self.fraud_df)
        self.fraud_utils_obj.join_csv(self.df_transactions)
        self.fraud_utils_obj.add_category_column(column_name, condition_column)

        return self.fraud_utils_obj.frauds_df
    
