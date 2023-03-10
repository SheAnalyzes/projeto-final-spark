#inicializando sessao

from pyspark.sql import SparkSession
from pyspark import SparkConf
#spark = SparkSession.builder.master("local[*]").appName("Teste").getOrCreate()

# Define a configuração para o driver JDBC
conf = SparkConf().setAppName("MyApp") \
    .setMaster("local[*]") \
    .set("spark.driver.extraClassPath", "/mnt/c/Users/Mariana/spark-3.3.2-bin-hadoop3/jars/mssql-jdbc-12.2.0.jre8.jar")

# Cria a sessão do Spark
spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


#class dataframe

from pyspark.sql.functions import col

class Data():
    '''A classe Dataframe tem o objetivo de proporcionar métodos capazes de ler arquivos CSVs, gerar dataframes e modificá-los.'''
     
    def __init__(self, csv_path,tipo): #mudar o nome da variavel "tipo"
        self.csv_path = csv_path
        self.tipo = tipo
        
    def criar(self):
        '''O método criar procura ler uma pasta que contém arquivo CSV gerando um dataframe.'''
        self.path_completo = self.csv_path + self.tipo + '-0**.csv'
        print(self.path_completo)
        
        #Lendo o arquivo CSV bruto
        try:
            self.df_bruto = spark.read.csv(self.path_completo, sep=";", header=False, inferSchema=True)
        except:
            print('O path nao existe. Por favor, insira um path valido.\nPode ser que o "tipo" passado nao seja "clients", "transaction_in" ou "transaction_out".')
            return 0
            
        # Filtrando o header. Note que está considerando que se tiverem vários headers, apenas 1 será selecionado
        self.header = self.df_bruto.filter(col("_c0") == "id").limit(1)
        print(self.header)
        
        # Excluindo o/os header do Dataframe bruto
        df_filtrado = self.df_bruto.where((col("_c0") != "id"))

        # Convertendo a primeira linha do header em uma lista de strings
        header_columns = self.header.first()

        # Gerando um dataframe com o header certo
        self.df = df_filtrado.toDF(*header_columns)
        print(self.df.count())

        return self.df
    
csv_path = '/mnt/c/Users/Mariana/Desktop/curso-azure/Projeto-Final/files/'

df_transacoes = Data(csv_path,'transaction*').criar()

#verificando as fraudes
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp,when
from pyspark.sql.types import TimestampType

# Particionando a tabela Transacoes (in e out) pela coluna "cliente_id" e ordenando por "data"
windowSpec  = Window.partitionBy("cliente_id").orderBy("data")

# Usando lag function para criar uma nova coluna (baseada na coluna "data") com a informacao da linha anterior a linha atual
df = df_transacoes.withColumn("data_anterior",lag("data",1).over(windowSpec))

# Transformando as datas ("data" e "data_anterior") em segundos
df = df\
    .withColumn("data_segundos", unix_timestamp("data", "yyyy-MM-dd HH:mm:ss Z"))\
    .withColumn("data_anterior_segundos", unix_timestamp("data_anterior", "yyyy-MM-dd HH:mm:ss Z"))

# Calculando a diferenca entre as linhas anteriores
df = df.withColumn("diff", col("data_segundos") - col("data_anterior_segundos"))

# Criando coluna que determina se houve fraude
df = df.withColumn("fraudes", when(col("diff") < 120, 'Fraude').otherwise('Sem fraude'))

# Reorganizando o Dataframe
# 1 - Une as informações dos dataframes df e df_transacoes com base nas colunas "id", "cliente_id", "valor" e "data"
df_fraudes = df.join(df_transacoes, ["id", "cliente_id", "valor", "data"], "left_outer")

# 2 - Seleciona as colunas desejadas
df_fraudes = df_fraudes.select(col("id"), col("cliente_id"), col("valor"), col("data"), col("fraudes"))
'''
#testando o banco
import pyodbc

pyodbc.drivers()
print(pyodbc.drivers())
driver = [item for item in pyodbc.drivers()][-1]
#driver = drivers[0]
print("driver:{}".format(driver))
server = 'she-analyzes.database.windows.net,1433'
database = 'projeto-final'
uid = 'administrador'
pwd = 'Sheadm8!'
con_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}'
print(con_string)
cnxn = pyodbc.connect(con_string)

#pyodbc.drivers()
#conn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};Server=tcp:she-analyzes.database.windows.net,1433;Database=projeto-final;Uid=administrador;Pwd=Sheadm8!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")    
print("Conexão com banco de dados estabelecida...")
input("Digite enter para continuar...")

conn.execute("DROP TABLE IF EXISTS Fraudes")
conn.execute("CREATE TABLE Fraudes (id int, cliente_id int, valor float, data datatime, fraudes varchar(50))")
print("Tabela criada em seu banco de dados...")

for index, row in df_fraudes.iterrows():
    conn.execute("INSERT INTO Fraudes (id, cliente_id, valor, data, fraudes) VALUES(?, ?, ?, ?, ?)", row[0], row[1], row[2], row[3], row[4])

print("Informações inseridas...")
input("Digite enter para continuar...")

conn.commit()
print("Importação executada!")
input("Digite enter para continuar...")

conn.close()
print("Conexão com banco de dados finalizada.")
input("Digite enter para continuar...")
'''

# Configurações do JDBC
jdbc_url = "jdbc:sqlserver://she-analyzes.database.windows.net:1433"
table_name = "Fraudes"
username = "administrador@she-analyzes"
password = "Sheadm8!"
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# Escreve o DataFrame no SQL Server usando JDBC
df_fraudes.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("database", 'projeto-final')\
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", driver) \
    .mode("append") \
    .save()

# Fecha a sessão do Spark
spark.stop()









