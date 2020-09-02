from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, IntegerType, FloatType,
                               StringType, StructField, StructType, TimestampType)


spark = SparkSession.builder.appName("Read EstudiantesJoin").getOrCreate()

estudiantes_schema = StructType([StructField('Carnet', IntegerType()),
                                 StructField('Nombre', StringType()),
                                 StructField('Carrera', StringType())])

estudiantes_df = spark.read.csv("estudiantes.csv",
                                schema=estudiantes_schema,
                                header=False)

# estudiantes_df.printSchema()
# estudiantes_df.show()

cursos_schema = StructType([StructField('CodigoCurso', IntegerType()),
                            StructField('Creditos', IntegerType()),
                            StructField('NombreCurso', StringType())])

cursos_df = spark.read.csv('cursos.csv',
                           schema=cursos_schema,
                           header=False)

# cursos_df.printSchema()
# cursos_df.show()

notas_schema = StructType([StructField('Carnet', IntegerType()),
                           StructField('CodigoCurso', IntegerType()),
                           StructField('Nota', FloatType())])

notas_df = spark.read.csv('notas.csv',
                          schema=notas_schema,
                          header=False)

# notas_df.printSchema()
# notas_df.show()


def join_dataframes(estudiantes_df, notas_df, cursos_df, notas_head, cursos_head):

    joint_df = estudiantes_df.join(notas_df, on=notas_head)
    # joint_df.printSchema()
    # joint_df.show()

    jointFinal_df = joint_df.join(cursos_df, on=cursos_head)
    # jointFinal_df.printSchema()
    # jointFinal_df.show()

    return jointFinal_df
