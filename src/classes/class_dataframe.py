from pyspark.sql.functions import col, to_timestamp, round
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql import Row
from classes.dataframe_cleaner import DataframeCleaner
from pyspark.sql.utils import AnalysisException



class CsvDataframe():
    '''The Dataframe class aims to provide methods capable of reading CSV files, generating dataframes, and modifying them.'''

    def __init__(self, csv_path, category):
        self.csv_path = csv_path
        self.category = category
        
    def _add_header(self):
        '''The "_add_header" method performs a well-defined process to add the correct header to the dataframe.'''

        # Finding the row that will be the header and saving it to a dataframe.
        self.header = self.raw_df.filter(col("_c0") == "id").limit(1)

        # Excluding from the raw dataframe the row that contains its future header. 
        filtered_df = self.raw_df.where((col("_c0") != "id"))

        # Generating a dataframe with the correct header.
        header_columns = self.header.first()
        self.fixed_df = filtered_df.toDF(*header_columns)

        return self.fixed_df
    
    def _fix_schema(self, df):
        '''The "_fix_schema" method aims to create a series of actions that, at the end, fixes the schematype of the dataframe generated from any CSV file.'''
        
        first_row = df.first()
        row = Row(*first_row)
        schema = df.schema


        for value in first_row:

            # Get the position of the value in the row's list of values
            value_position = row.index(value)

            # Get the column name from the position
            column_name = schema[value_position].name
            
            # Check if the column type need to be converted to datatime 
            if isinstance(value, str) and ':' in value:
                df = df.withColumn(column_name, to_timestamp(col(column_name), "yyyy-MM-dd HH:mm:ss Z"))
            
            # Check if the column type need to be converted to an integer
            elif isinstance(value, str) and value.isdigit():
                df = df.withColumn(column_name, col(column_name).cast(IntegerType()))
            
            # Check if the column type needs to be converted to float and limit to 2 decimal places
            elif isinstance(value, str) and '.' in value and '@' not in value:
                df = df.withColumn(column_name, round(col(column_name).cast(FloatType()), 2))
                
        return df

    def read_csv(self, spark):
        '''The "read_csv" method reads a folder that contains CSV files, initially generating a dataframe without header and then returning a dataframe with it.'''

        self.complete_path = self.csv_path + self.category + '.csv'
        print(self.complete_path)

        try:
            self.raw_df = spark.read.csv(self.complete_path, sep=";", header=False, inferSchema=True)
        except:
            print('The path does not exist. Please, insert a valid path.\nThe "category" passed might not be "clients", "transaction_in" or "transaction_out" or similar.')
            return 0
            
        # Fix and clean the dataframe
        self.df = self._add_header()
        self.df = self._fix_schema(self.df)
        self.df = self.clean_data(self.df)
        return self.df
    
    def clean_data(self, df):
        '''This method cleans the dataframe based in a collection of methods of DataframeUtils class.'''
        
        cleaner_df = DataframeCleaner.drop_empty_rows(df)
        cleaner_df = DataframeCleaner.drop_duplicate(cleaner_df)

        # Only drop empty "valor" columns of transactions
        if self.category != 'clients*':
            cleaner_df = DataframeCleaner.drop_empty_valor(cleaner_df, "valor")

        return cleaner_df