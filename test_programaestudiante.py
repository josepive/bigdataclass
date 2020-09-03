from .programaestudiante import join_dataframes, aggregate_dataframe, top_dataframe


# Prueba de join de columnas de DataFrames
def test_multicolumn_join(spark_session):
    # Datos de prueba de estudiantes
    estudiante_data = [(12345, 'Juan Perez', 'SOC'),
                       (27890, 'Laura Chinchilla', 'COM'),
                       (33456, 'Luisa Lane', 'BIO'),
                       (43987, 'Eduardo Soto', 'PHI'),
                       (58219, 'Warner Sanchez', 'ICV'),
                       (61234, 'Marco Alvarado', 'IEL')]

    estudiante_ds = spark_session.createDataFrame(
        estudiante_data, ['Carnet', 'Nombre', 'Carrera'])

    # Datos de prueba de cursos
    curso_data = [(1001, 4, 'SOC'),
                  (1002, 4, 'SOC'),
                  (2001, 2, 'COM'),
                  (2002, 3, 'COM'),
                  (3001, 4, 'BIO'),
                  (3002, 4, 'BIO'),
                  (4001, 1, 'PHI'),
                  (4002, 3, 'PHI'),
                  (5001, 4, 'ICV'),
                  (5002, 5, 'ICV'),
                  (6001, 4, 'IEL'),
                  (6002, 4, 'IEL')]

    curso_ds = spark_session.createDataFrame(
        curso_data, ['Curso', 'Creditos', 'Carrera'])

    # Datos de prueba de notas
    nota_data = [(12345, 1001, 80),
                 (12345, 1002, 90),
                 (27890, 2001, 90),
                 (27890, 2002, 85),
                 (33456, 3001, 0),
                 (43987, 4001, 95),
                 (43987, 4002, 80),
                 (58219, 5001, 60),
                 (61234, 6001, 70),
                 (61234, 6002, 45),
                 (61234, 6002, 75)]

    nota_ds = spark_session.createDataFrame(
        nota_data, ['Carnet', 'Curso', 'Nota'])

    estudiante_ds.show()
    curso_ds.show()
    nota_ds.show()

    actual_ds = join_dataframes(estudiante_ds, curso_ds, nota_ds)

    expected_ds = spark_session.createDataFrame(
        [(12345, 'Juan Perez', 'SOC', 1001, 80, 4),
         (12345, 'Juan Perez', 'SOC', 1002, 90, 4),
         (27890, 'Laura Chinchilla', 'COM', 2001, 90, 2),
         (27890, 'Laura Chinchilla', 'COM', 2002, 85, 3),
         (33456, 'Luisa Lane', 'BIO', 3001,  0, 4),
         (43987, 'Eduardo Soto', 'PHI', 4001, 95, 1),
         (43987, 'Eduardo Soto', 'PHI', 4002, 80, 3),
         (58219, 'Warner Sanchez', 'ICV', 5001, 60, 4),
         (61234, 'Marco Alvarado', 'IEL', 6001, 70, 4),
         (61234, 'Marco Alvarado', 'IEL', 6002, 45, 4),
         (61234, 'Marco Alvarado', 'IEL', 6002, 75, 4)],
        ['Carnet', 'Nombre', 'Carrera', 'Curso', 'Nota', 'Creditos'])

    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()


# Prueba de agregaciones parciales
def test_aggregation(spark_session):
    # Datos de prueba de estudiantes
    estudiante_data = [(12345, 'Juan Perez', 'SOC'),
                       (27890, 'Laura Chinchilla', 'COM'),
                       (33456, 'Luisa Lane', 'BIO'),
                       (43987, 'Eduardo Soto', 'PHI'),
                       (58219, 'Warner Sanchez', 'ICV'),
                       (61234, 'Marco Alvarado', 'IEL')]

    estudiante_ds = spark_session.createDataFrame(
        estudiante_data, ['Carnet', 'Nombre', 'Carrera'])

    # Datos de prueba de cursos
    curso_data = [(1001, 4, 'SOC'),
                  (1002, 4, 'SOC'),
                  (2001, 2, 'COM'),
                  (2002, 3, 'COM'),
                  (3001, 4, 'BIO'),
                  (3002, 4, 'BIO'),
                  (4001, 1, 'PHI'),
                  (4002, 3, 'PHI'),
                  (5001, 4, 'ICV'),
                  (5002, 5, 'ICV'),
                  (6001, 4, 'IEL'),
                  (6002, 4, 'IEL')]

    curso_ds = spark_session.createDataFrame(
        curso_data, ['Curso', 'Creditos', 'Carrera'])

    # Datos de prueba de notas
    nota_data = [(12345, 1001, 80),
                 (12345, 1002, 90),
                 (27890, 2001, 90),
                 (27890, 2002, 85),
                 (33456, 3001, 0),
                 (43987, 4001, 95),
                 (43987, 4002, 80),
                 (58219, 5001, 60),
                 (61234, 6001, 70),
                 (61234, 6002, 45),
                 (61234, 6002, 75)]

    nota_ds = spark_session.createDataFrame(
        nota_data, ['Carnet', 'Curso', 'Nota'])

    #estudiante_ds.show()
    #curso_ds.show()
    #nota_ds.show()

    joint_df = join_dataframes(estudiante_ds, curso_ds, nota_ds)

    actual_ds = aggregate_dataframe(joint_df)

    expected_ds = spark_session.createDataFrame(
        [(12345, 'Juan Perez', 'SOC', 85.0),
         (27890, 'Laura Chinchilla', 'COM', 87.0),
         (33456, 'Luisa Lane', 'BIO', 0.0),
         (43987, 'Eduardo Soto', 'PHI', 83.75),
         (58219, 'Warner Sanchez', 'ICV', 60.0),
         (61234, 'Marco Alvarado', 'IEL', 63.33)],
        ['Carnet', 'Nombre', 'Carrera', 'Promedio'])

    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()


# Prueba de resultados finales
def test_top(spark_session):
    # Datos de prueba de estudiantes
    estudiante_data = [(12345, 'Juan Perez', 'SOC'),
                       (27890, 'Laura Chinchilla', 'COM'),
                       (33456, 'Luisa Lane', 'BIO'),
                       (43987, 'Eduardo Soto', 'PHI'),
                       (58219, 'Warner Sanchez', 'ICV'),
                       (61234, 'Marco Alvarado', 'IEL')]

    estudiante_ds = spark_session.createDataFrame(
        estudiante_data, ['Carnet', 'Nombre', 'Carrera'])

    # Datos de prueba de cursos
    curso_data = [(1001, 4, 'SOC'),
                  (1002, 4, 'SOC'),
                  (2001, 2, 'COM'),
                  (2002, 3, 'COM'),
                  (3001, 4, 'BIO'),
                  (3002, 4, 'BIO'),
                  (4001, 1, 'PHI'),
                  (4002, 3, 'PHI'),
                  (5001, 4, 'ICV'),
                  (5002, 5, 'ICV'),
                  (6001, 4, 'IEL'),
                  (6002, 4, 'IEL')]

    curso_ds = spark_session.createDataFrame(
        curso_data, ['Curso', 'Creditos', 'Carrera'])

    # Datos de prueba de notas
    nota_data = [(12345, 1001, 80),
                 (12345, 1002, 90),
                 (27890, 2001, 90),
                 (27890, 2002, 85),
                 (33456, 3001, 0),
                 (43987, 4001, 95),
                 (43987, 4002, 80),
                 (58219, 5001, 60),
                 (61234, 6001, 70),
                 (61234, 6002, 45),
                 (61234, 6002, 75)]

    nota_ds = spark_session.createDataFrame(
        nota_data, ['Carnet', 'Curso', 'Nota'])

    joint_df = join_dataframes(estudiante_ds, curso_ds, nota_ds)

    aggregated_df = aggregate_dataframe(joint_df)

    actual_ds = top_dataframe(aggregated_df)

    expected_ds = spark_session.createDataFrame(
        [(27890, 'Laura Chinchilla', 'COM', 87.0),
         (12345, 'Juan Perez', 'SOC', 85.0),
         (43987, 'Eduardo Soto', 'PHI', 83.75),
         (61234, 'Marco Alvarado', 'IEL', 63.33),
         (58219, 'Warner Sanchez', 'ICV', 60.0),
         (33456, 'Luisa Lane', 'BIO', 0.0)],
        ['Carnet', 'Nombre', 'Carrera', 'Promedio'])

    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()
