from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)


spark = SparkSession.builder.appName("Read EstudiantesJoin").getOrCreate()

csv_schema = StructType([StructField('Carnet', IntegerType()),
                         StructField('Nombre', StringType()),
                         StructField('Carrera', StringType()),
                         ])

estudiantes_df = spark.read.csv("estudiantes.csv",
                           schema=csv_schema,
                           header=False)

#estudiantes_df.show()



# Load separate file para los cursos
cursos_schema = StructType([StructField('CodigoCurso', IntegerType()),
                         StructField('Creditos', IntegerType()),
                         StructField('Carrera', StringType()),
                         ])

cursos_df = spark.read.csv('cursos.csv',
                          schema=cursos_schema,
                          header=False)

#cursos_df.printSchema()
#cursos_df.show()

# Load separate file para las notas
notas_schema = StructType([StructField('Carnet', IntegerType()),
                         StructField('CodigoCurso', IntegerType()),
                         StructField('Nota', FloatType()),
                         ])

notas_df = spark.read.csv('notas.csv',
                          schema=notas_schema,
                          header=False)

#notas_df.printSchema()
#notas_df.show()

# ...and join to the aggregates
'''
joint_df = stats_df.join(names_df, stats_df.customer_id == names_df.id)
joint_df.printSchema()
joint_df.show()
'''
def join_dataframes(estudiantes_df,notas_df,cursos_df,estudiantes_head,notas_head,cursos_head):

    joint_df = estudiantes_df.join(notas_df, estudiantes_df.Carnet == notas_df.Carnet)
    #joint_df.printSchema()
    #joint_df.show()

    jointFinal_df = joint_df.join(cursos_df, joint_df.CodigoCurso == cursos_df.CodigoCurso)
    #jointFinal_df.printSchema()
    #jointFinal_df.show()   

    return jointFinal_df