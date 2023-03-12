class Database():
    '''
    The Database class aims to perform actions that manipulate tables and data from a specific database.
    Attention: To perform these actions, you need to connect through JDBC.
    '''

    def __init__(self, *, jdbc_url, database, username, password, driver):
        self.jdbc_url = jdbc_url
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver

    def create_table(self, df, table_name):
        '''The "create_table" method aims to create a table in the specified database from an already defined dataframe.'''
        
        self.df = df
        self.table_name = table_name

        
        # Tries to create a table in the specified database.
        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("database", self.database)\
            .option("dbtable", self.table_name) \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("driver", self.driver) \
            .mode("overwrite") \
            .save()

        def show_table (self):
            '''This method aims to show the tables already created in the database.'''
            pass