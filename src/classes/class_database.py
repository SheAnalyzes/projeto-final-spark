from pyspark.sql.utils import AnalysisException

class Database():
    '''
    A classe Database procura realizar ações que manupulam as tabelas e dados de um database específico.
    Atenção: Para realizar essas ações, é preciso se conectar através do JDBC.
    '''

    def __init__(self, *, jdbc_url, database, username, password, driver):
        self.jdbc_url = jdbc_url
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver

    def criar_tabela(self, df, nome_tabela):
        '''O método "criar_tabela" procura criar uma tabela no database especificado a partir de um dataframe já definido.'''
        self.df = df
        self.nome_tabela = nome_tabela

        
        # Tenta criar uma tabela no database especificado.
        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("database", self.database)\
            .option("dbtable", self.nome_tabela) \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("driver", self.driver) \
            .mode("overwrite") \
            .save()

    def atualizar_tabela():
        pass

    def remover_tabela():
        pass
    