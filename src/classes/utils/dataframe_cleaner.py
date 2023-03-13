from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace
 
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
    
    @staticmethod
    def format_lowercase(df: DataFrame, column_name: str) -> DataFrame:
        '''This method formats a column in a Spark DataFrame to the lower case format.'''

        df = df.withColumn(column_name, lower(col(column_name)))
        return df
    
    @staticmethod
    def remove_extra_space(df: DataFrame,column_name: str) -> DataFrame:
        '''This method removes the extra empty space inside the column.'''

        df = df.withColumn(column_name, trim(regexp_replace(col(column_name), "\s+", " ")))
        return df
    
    @staticmethod
    def remove_accents(df: DataFrame, column_name: str) -> DataFrame:
        '''This method removes the accent in some rows of the "column_name".'''

        df = df.withColumn(column_name, regexp_replace(column_name, "[áàâãä]", "a"))
        df = df.withColumn(column_name, regexp_replace(column_name, "[éèêë]", "e"))
        df = df.withColumn(column_name, regexp_replace(column_name, "[íìîï]", "i"))
        df = df.withColumn(column_name, regexp_replace(column_name, "[óòôõö]", "o"))
        df = df.withColumn(column_name, regexp_replace(column_name, "[úùûü]", "u"))
        df = df.withColumn(column_name, regexp_replace(column_name, "[ç]", "c"))
        return df

