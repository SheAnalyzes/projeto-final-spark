from pyspark.sql import SparkSession
from pyspark import SparkConf

class PySpark():
    '''A classe Spark tem o objetivo de proporcionar métodos capazes de iniciar e encerrar sessões do PySpark.'''
    def __init__(self, path_jdbc_driver):
        self.path_jdbc_driver = path_jdbc_driver
        
        # Define a configuração para o driver JDBC
        self.conf = SparkConf().setAppName("ETL") \
            .setMaster("local[*]") \
            .set("spark.driver.extraClassPath", self.path_jdbc_driver)

    def iniciar_sessao(self):
        '''Este método inicia a sessão PySpark considerando o uso futuro de métodos que utilizam o JDBC, retornando o objeto "spark".'''
        # Cria a sessão do Spark
        self.spark = SparkSession.builder \
            .config(conf=self.conf) \
            .getOrCreate()
        return self.spark
    
    def encerrar_sessao(self):
        '''Este método encerra a sessão PySpark.'''
        self.spark.stop()