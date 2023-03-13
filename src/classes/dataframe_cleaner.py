from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class DataframeCleaner:
    '''"The DataframeCleaner class provides methods to clean and transform dataframes and assists the CsvDataframe class.'''
    
    @staticmethod
    def drop_empty_rows(df: DataFrame) -> DataFrame:
        """This method removes empty rows from the dataframe."""

        return df.dropna(how='all', subset=df.columns)

    @staticmethod
    def drop_duplicate(df: DataFrame) -> DataFrame:
        '''This method removes all the duplicate rows from the dataframe.'''

        return df.dropDuplicates()

    @staticmethod
    def drop_empty_valor(df: DataFrame, col_name: str) -> DataFrame:
        '''This method removes all the rows that have empty values in the column 'valor'.'''

        return df.filter(col(col_name).isNotNull())