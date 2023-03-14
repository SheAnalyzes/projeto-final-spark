from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace
 
class DataframeCleaner:
    '''"The DataframeCleaner class provides methods to clean and transform dataframes and assists the CsvDataframe class.'''
    df = None

    def __init__ (self,df):
        self.df = df

    def drop_empty_rows(self):
        """This method removes empty rows from the dataframe."""

        self.df.dropna(how='all', subset=self.df.columns)

    def drop_duplicate(self):
        '''This method removes all the duplicate rows from the dataframe.'''

        self.df.dropDuplicates()

    def drop_empty_valor(self, col_name):
        '''This method removes all the rows that have empty values in the column 'valor'.'''

        self.df.filter(col(col_name).isNotNull())
    
    def format_lowercase(self, column_name):
        '''This method formats a column in a Spark DataFrame to the lower case format.'''

        self.df = self.df.withColumn(column_name, lower(col(column_name)))
    
    def remove_extra_space(self,column_name):
        '''This method removes the extra empty space inside the column.'''

        self.df = self.df.withColumn(column_name, trim(regexp_replace(col(column_name), "\s+", " ")))
     
    def remove_accents(self, column_name):
        '''This method removes the accent in some rows of the "column_name".'''

        self.df = self.df.withColumn(column_name, regexp_replace(column_name, "[áàâãä]", "a"))
        self.df = self.df.withColumn(column_name, regexp_replace(column_name, "[éèêë]", "e"))
        self.df = self.df.withColumn(column_name, regexp_replace(column_name, "[íìîï]", "i"))
        self.df = self.df.withColumn(column_name, regexp_replace(column_name, "[óòôõö]", "o"))
        self.df = self.df.withColumn(column_name, regexp_replace(column_name, "[úùûü]", "u"))
        self.df = self.df.withColumn(column_name, regexp_replace(column_name, "[ç]", "c"))

