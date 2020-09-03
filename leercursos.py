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
    joint_df.printSchema()
    joint_df.show()

    jointFinal_df = joint_df.join(cursos_df, on=cursos_head)
    jointFinal_df.printSchema()
    jointFinal_df.show()

    return jointFinal_df

actual_df = join_dataframes(estudiantes_df, notas_df, cursos_df,
                                ['Carnet'], ['CodigoCurso'])
print(actual_df)


def obt_JoinTotalLimpio(estudiantes_df, cursos_df, notas_df):

    joint_df = estudiantes_df.join(notas_df, estudiantes_df.Carnet == notas_df.Carnet)

    repeated_columns = [c for c in estudiantes_df.columns if c in notas_df.columns]
    for col in repeated_columns:
        joint_df = joint_df.drop(notas_df[col])

    jointFinal_df = joint_df.join(cursos_df, joint_df.CodigoCursos == curso_df.CodigoCurso)

    repeated_columns2 = [c for c in joint_df.columns if c in curso_df.columns]
    for col in repeated_columns2:
        jointFinal_df = jointFinal_df.drop(curso_df[col])

    jointFinal_df.show()
    return jointFinal_df  

def obt_MejorEstudiante(join_df):


    jointFinalProm_df = join_df.withColumn("PromedioPond", join_df.Creditos*join_df.Nota)
    
    jointFinalProm_df = jointFinalProm_df.groupBy("Carnet").sum()

    jointFinalProm2_df = jointFinalProm_df.select(col('Carnet'), col('sum(PromedioPond)').alias('Promedio'),
                                                    col('sum(Creditos)').alias('CreditosTot')).sort(desc('Promedio'))
    jointFinalProm2_df.show()
    return jointFinalProm2_df
