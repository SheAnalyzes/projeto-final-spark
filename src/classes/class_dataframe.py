from pyspark.sql.functions import col
from pyspark.sql.functions import col, to_timestamp, format_number
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql import Row



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

    def _corrigir_schema(self, df):

        primeira_linha = df.first()
        
        row = Row(*primeira_linha)

        esquema = df.schema

        # Converte o objeto Row em uma lista e itera sobre ela
        for valor in primeira_linha:
            # obtém a posição do valor na lista de valores da linha
            posicao_valor = row.index(valor)

            # obtém o nome da coluna a partir da posição
            nome_coluna = esquema[posicao_valor].name
            if isinstance (valor, str) and ':' in valor:
                # Use o método strptime() para converter a string em um objeto datetime
                #converte as colunas data em timestamp, mas se ela não for convertivel, permanece o valor original
               df = df.withColumn(nome_coluna, to_timestamp(col(nome_coluna), "yyyy-MM-dd HH:mm:ss Z"))
             
            elif isinstance(valor, str) and valor.isdigit():
                #converte o tipo da coluna para int
                df = df.withColumn(nome_coluna, col(nome_coluna).cast(IntegerType()))
            elif isinstance(valor,str) and '.' in valor and '@' not in valor:
                #converte o tipo da coluna para int
                df = df.withColumn(nome_coluna, col(nome_coluna).cast(FloatType()))
                df = df.withColumn(nome_coluna, format_number(col(nome_coluna), 2))
        return df

    def ler_csv(self, spark):
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
        self.df = self._corrigir_schema(self.df)
        return self.df
    
    def juntar_csv(self,df_col_fraudes, df_transacoes):
        # Une as informações dos dataframes df e df_transacoes com base nas colunas "id", "cliente_id", "valor" e "data"
        self.temp_fraudes = df_col_fraudes.join(df_transacoes, ["id", "cliente_id", "valor", "data"], "left_outer")

        # Seleciona as colunas desejadas
        self.temp_fraudes = self.temp_fraudes.select(col("id"), col("cliente_id"), col("valor"), col("data"), col("suspeita_de_fraude"))

        return self.temp_fraudes