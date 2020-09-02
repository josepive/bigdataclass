from .leercursos import join_dataframes


def test_multicolumn_join(spark_session):
    estudiantes_data = [(12345, 'Juan Perez', 'Sociologia'),
                        (67890, 'Laura Chinchilla', 'Comunicacion'),
                        (23456, 'Luisa Lane Sanchez', 'Biologia'),
                        (23987, 'Eduardo Soto Jimenez', 'Filologia'),
                        (78219, 'Warner Sanchez Saborio', 'Ing. Civil'),
                        (91234, 'Marco Alvarado Rojas', 'Ing. Electrica')]

    estudiantes_ds = spark_session.createDataFrame(
        estudiantes_data, ['Carnet', 'Nombre', 'Carrera'])

    notas_data = [(12345, 1001, 80), (67890, 1002, 60), (23456, 1003, 85),
                  (23987, 1004, 90), (78219, 1005, 75), (91234, 1006, 70)]

    notas_ds = spark_session.createDataFrame(
        notas_data, ['Carnet', 'CodigoCurso', 'Nota'])

    cursos_data = [(1001, 4, 'Int. a Sociologia'), (1002, 2, 'Comunicacion'),
                   (1003, 4, 'Matematica General'), (1004, 1, 'Actividad Deportiva'),
                   (1005, 4, 'Estatica'), (1006, 4, 'Circuitos en CC')]

    cursos_ds = spark_session.createDataFrame(
        cursos_data, ['CodigoCurso', 'Creditos', 'NombreCurso'])

    estudiantes_ds.show()
    notas_ds.show()
    cursos_ds.show()

    actual_ds = join_dataframes(estudiantes_ds, notas_ds, cursos_ds,
                                ['Carnet'], ['CodigoCurso'])

    expected_ds = spark_session.createDataFrame(
        [(1002, 67890, 'Laura Chinchilla', 'Comunicacion', 60, 2, 'Comunicacion'),
         (1005, 78219, 'Warner Sanchez Saborio', 'Ing. Civil', 75, 4, 'Estatica'),
         (1001, 12345, 'Juan Perez', 'Sociologia', 80, 4, 'Int. a Sociologia'),
         (1004, 23987, 'Eduardo Soto Jimenez',
          'Filologia', 90, 1, 'Actividad Deportiva'),
         (1006, 91234, 'Marco Alvarado Rojas',
          'Ing. Electrica', 70, 4, 'Circuitos en CC'),
         (1003, 23456, 'Luisa Lane Sanchez', 'Biologia', 85, 4, 'Matematica General')],
        ['CodigoCurso', 'Carnet', 'Nombre', 'Carrera', 'Nota', 'Creditos', 'NombreCurso'])


    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()
