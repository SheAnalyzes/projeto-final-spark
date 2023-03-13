from pyspark.sql import SparkSession
from pyspark import SparkConf

class PySpark():
    '''The Spark class aims to provide methods capable of starting and ending PySpark sessions.'''

    def __init__(self, path_jdbc_driver):
        self.path_jdbc_driver = path_jdbc_driver
        
        self.conf = SparkConf().setAppName("ETL") \
            .setMaster("local[*]") \
            .set("spark.driver.extraClassPath", self.path_jdbc_driver)

    def start_session(self):
        '''This method starts the PySpark session configuring the jdbc driver.'''
    
        self.spark = SparkSession.builder \
            .config(conf=self.conf) \
            .getOrCreate()
        return self.spark
    
    def end_session(self):
        '''This method ends the PySpark session.'''

        self.spark.stop()