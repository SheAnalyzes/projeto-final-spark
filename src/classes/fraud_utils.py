from pyspark.sql.functions import col, when
from pyspark.sql import DataFrame

class FraudUtils:
    '''The FraudUtils class provides methods to modify dataframes that assist the Fraud class in creating fraud-related dataframes.'''

    @staticmethod
    def join_csv(df_fraud_cols: DataFrame, df_transactions: DataFrame) -> DataFrame:
        '''The "join_csv" method aims to merge two dataframes and filter the final dataframe to only include specific columns.'''

        temp_frauds = df_fraud_cols.join(df_transactions, ["id", "cliente_id", "valor", "data"], "left_outer")

        temp_frauds = temp_frauds.select(col("id"), col("cliente_id"), col("valor"), col("data"), col("suspeita_de_fraude"))

        return temp_frauds

    @staticmethod
    def add_category_column(column_name: str, condition_column: str, df: DataFrame) -> DataFrame:
        '''This method aims to add a column inside the dataframe based in some "when" condition.'''

        df = df.withColumn(column_name, when(col(condition_column) > 0, 'entrada').otherwise('saida'))
        return df