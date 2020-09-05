from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, IntegerType, FloatType,
                               StringType, StructField, StructType, TimestampType)
from pyspark.sql.functions import sum, round, when, count


def join_dataframes(estudiante_df, curso_df, nota_df):
    estudiante_nota_df = estudiante_df.join(nota_df, on='Carnet')

    # Se ignora columna de carrera, que ya está en estudiante_nota_df
    curso_1_df = curso_df.select('Curso', 'Creditos')

    estudiante_nota_curso_df = estudiante_nota_df.join(curso_1_df, on='Curso')

    joint_df = estudiante_nota_curso_df.select(
        'Carnet', 'Nombre', 'Carrera', 'Curso', 'Nota', 'Creditos').sort('Carnet', 'Curso')

    return joint_df


def aggregate_dataframe(joint_df):
    joint_1_df = joint_df.select('Carnet', 'Nota', 'Creditos')

    weighted_average_df = joint_1_df.groupBy('Carnet').agg(round(sum(joint_df.Nota * joint_df.Creditos) / (
        when(sum(joint_df.Creditos) == 0, count(joint_df.Creditos))).otherwise(sum(joint_df.Creditos)), 2).alias('Promedio'))

    aggregated_df = joint_df.select('Carnet', 'Nombre', 'Carrera').distinct().join(
        weighted_average_df, 'Carnet').sort('Carnet')

    return aggregated_df


def top_dataframe(aggregated_df):
    return aggregated_df.sort('Carrera').sort('Promedio', ascending=False)


if __name__ == '__main__':
    import sys

    spark = SparkSession.builder.appName('Mejores Promedios').getOrCreate()

    # Archivo de estudiantes

    estudiante_schema = StructType([StructField('Carnet', IntegerType()),
                                    StructField('Nombre', StringType()),
                                    StructField('Carrera', StringType())])

    estudiante_df = spark.read.csv(sys.argv[1],
                                   schema=estudiante_schema,
                                   header=False)

    estudiante_df.show()

    # Archivo de cursos

    curso_schema = StructType([StructField('Curso', IntegerType()),
                               StructField('Creditos', IntegerType()),
                               StructField('Carrera', StringType())])

    curso_df = spark.read.csv(sys.argv[2],
                              schema=curso_schema,
                              header=False)

    curso_df.show()

    # Archivo de notas

    nota_schema = StructType([StructField('Carnet', IntegerType()),
                              StructField('Curso', IntegerType()),
                              StructField('Nota', FloatType())])

    nota_df = spark.read.csv(sys.argv[3],
                             schema=nota_schema,
                             header=False)

    nota_df.show()

    # Mejores promedios

    joint_df = join_dataframes(estudiante_df, curso_df, nota_df)

    joint_df.show()

    aggregated_df = aggregate_dataframe(joint_df)

    aggregated_df.show()

    top_df = top_dataframe(aggregated_df)

    top_df.show()
