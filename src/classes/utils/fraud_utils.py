from pyspark.sql.functions import col, when
from pyspark.sql import DataFrame

class FraudUtils:
    '''The FraudUtils class provides methods to modify dataframes that assist the Fraud class in creating fraud-related dataframes.'''
    frauds_df = None

    def __init__ (self, frauds_df):
        self.frauds_df = frauds_df

    def join_csv(self,df_transactions):
        '''The "join_csv" method aims to merge two dataframes and filter the final dataframe to only include specific columns.'''

        self.frauds_df = self.frauds_df.join(df_transactions, ["id", "cliente_id", "valor", "data_transacao"], "left_outer")

        self.frauds_df = self.frauds_df.select(col("id"), col("cliente_id"), col("valor"), col("data_transacao"), col("fraude"))

    def add_category_column(self,column_name, condition_column):
        '''This method aims to add a column inside the dataframe based in some "when" condition.'''

        self.frauds_df = self.frauds_df.withColumn(column_name, when(col(condition_column) > 0, 'entrada').otherwise('saida'))
