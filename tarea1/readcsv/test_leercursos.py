from .joiner import join_dataframes


def test_one_column_join(spark_session):
    estudiantes_data = [(12345,'Juan Perez','Sociologia'), (67890,'Laura Chinchilla','Comunicacion'), (23456,'Luisa Lane Sanchez','Biologia'), (23987,'Eduardo Soto Jimenez','Filologia'), (78219,'Warner Sanchez Saborio','Ing. Civil'), (91234,'Marco Alvarado Rojas','Ing. Electrica')]
    
    estudiantes_ds = spark_session.createDataFrame(estudiantes_data,['Carnet', 'Nombre', 'Carrera'])
    
    notas_data = [(12345,001,80),(67890,002,60),(23456,003,85),(23987,004,90),(78219,005,75),(91234,006,70)]

    notas_ds = spark_session.createDataFrame(notas_data,['Carnet', 'CodigoCurso', 'Nota'])

    grades_ds.show()
    student_ds.show()

    actual_ds = join_dataframes(grades_ds, student_ds, ['student_id'], ['id'])

    expected_ds = spark_session.createDataFrame(
        [
            (1, 50, 1, 'Juan'),
            (2, 100, 2, 'Maria'),
        ],
        ['student_id', 'grade', 'id', 'name'])

    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()
