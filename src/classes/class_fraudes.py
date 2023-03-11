from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp, when, col

class Fraudes:
    '''A classe Fraudes procura identificar as fraudes presentes nos dataframes de transação bancária.'''
    
    def __init__ (self, df_transacoes):
        self.df_transacoes = df_transacoes

    def _identificar_fraudes(self):
        '''Este método identifica as fraudes e cria uma coluna a mais no dataframe original.'''
        
        # Particionando o df de transacoes (in e out) pela coluna "cliente_id" e ordenando por "data".
        windowSpec  = Window.partitionBy("cliente_id").orderBy("data")

        # Usando lag function para criar uma nova coluna (baseada na coluna "data") com a informacao da linha anterior a linha atual
        df = self.df_transacoes.withColumn("data_anterior",lag("data",1).over(windowSpec))

        # Transformando as datas ("data" e "data_anterior") em segundos
        df = df\
            .withColumn("data_segundos", unix_timestamp("data", "yyyy-MM-dd HH:mm:ss Z"))\
            .withColumn("data_anterior_segundos", unix_timestamp("data_anterior", "yyyy-MM-dd HH:mm:ss Z"))

        # Calculando a diferenca entre as linhas anteriores
        df = df.withColumn("diff", col("data_segundos") - col("data_anterior_segundos"))

        # Criando coluna que determina se houve fraude
        self.df = df.withColumn("fraudes", when(col("diff") < 120, 'Fraude').otherwise('Sem fraude'))

        return self.df
    
    def criar_df_fraudes(self):
        '''Este método cria dataframes que contém colunas sinalizando suspeita ou não de fraudes.'''        
        
        df_col_fraudes = self._identificar_fraudes()

        # Une as informações dos dataframes df e df_transacoes com base nas colunas "id", "cliente_id", "valor" e "data"
        self.df_fraudes = df_col_fraudes.join(self.df_transacoes, ["id", "cliente_id", "valor", "data"], "left_outer")

        # Seleciona as colunas desejadas
        self.df_fraudes = self.df_fraudes.select(col("id"), col("cliente_id"), col("valor"), col("data"), col("fraudes"))

        return self.df_fraudes