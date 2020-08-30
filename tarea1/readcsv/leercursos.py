from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("Basic Read and Print").getOrCreate()

csv_schema = StructType([StructField('codigo', IntegerType()),
                         StructField('creditos', IntegerType()),
                         StructField('carrera', StringType())])

dataframe_cursos = spark.read.csv("cursos.csv",
                           schema=csv_schema,
                           header=False)

dataframe_cursos.show()

# Join tarea

# Group By and Select the data already aggregated
#sum_df = typed_df.groupBy("customer_id", "date").sum()
#sum_df.show()