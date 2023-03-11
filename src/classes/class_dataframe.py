from pyspark.sql.functions import col

class Dataframe():
    '''A classe Dataframe tem o objetivo de proporcionar métodos capazes de ler arquivos CSVs, gerar dataframes e modificá-los.'''
     
    def __init__(self, csv_path,categoria):
        self.csv_path = csv_path
        self.categoria = categoria
        
    def _adicionar_header(self):
        '''O método "_adicionar_header" realiza algumas etapas para adicionar o header correto ao dataframe.'''

        # Encontrando a linha que será o header e salvando num dataframe.
        self.header = self.df_bruto.filter(col("_c0") == "id").limit(1)

        # Excluindo do dataframe bruto a linha que contém seu futuro header. 
        df_filtrado = self.df_bruto.where((col("_c0") != "id"))

        # Gerando um dataframe com o header certo.
        header_columns = self.header.first()
        self.df_corrigido = df_filtrado.toDF(*header_columns)

        return self.df_corrigido

    def criar(self, spark):
        '''O método "criar" lê uma pasta que contém arquivos CSVs, inicialmente gerando um dataframe sem header e posteriormente retornando um dataframe com header.'''

        self.path_completo = self.csv_path + self.categoria + '.csv'
        print(self.path_completo)
        # Lendo os arquivos CSVs dentro do path
        try:
            self.df_bruto = spark.read.csv(self.path_completo, sep=";", header=False, inferSchema=True)
        except:
            print('O path não existe. Por favor, insira um path válido.\nPode ser que a "categoria" passada não seja "clients", "transaction_in" ou "transaction_out".')
            return 0
        
        # Adicionando o header correto ao dataframe criado
        self.df = self._adicionar_header()

        return self.df