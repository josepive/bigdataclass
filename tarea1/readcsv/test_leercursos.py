from .leercursos import join_dataframes


def test_one_column_join(spark_session):
    estudiantes_data = [(12345,'Juan Perez','Sociologia'), (67890,'Laura Chinchilla','Comunicacion'), (23456,'Luisa Lane Sanchez','Biologia'), (23987,'Eduardo Soto Jimenez','Filologia'), (78219,'Warner Sanchez Saborio','Ing. Civil'), (91234,'Marco Alvarado Rojas','Ing. Electrica')]
    
    estudiantes_ds = spark_session.createDataFrame(estudiantes_data,['Carnet', 'Nombre', 'Carrera'])
    
    notas_data = [(12345,1001,80),(67890,1002,60),(23456,1003,85),(23987,1004,90),(78219,1005,75),(91234,1006,70)]

    notas_ds = spark_session.createDataFrame(notas_data,['Carnet', 'CodigoCurso', 'Nota'])

    cursos_data = [(1001,4,'Int. a Sociologia'),(1002,2,'Comunicacion'),(1003,4,'Matematica General'),(1004,1,'Actividad Deportiva'),(1005,4,'Estatica'),(1006,4,'Circuicos en CC')] 

    cursos_ds = spark_session.createDataFrame(cursos_data,['CodigoCurso', 'Creditos', 'Carrera'])


    estudiantes_ds.show()
    notas_ds.show()
    cursos_ds.show()

    actual_ds = join_dataframes(estudiantes_ds, notas_ds, cursos_ds, ['Carnet'], ['Carnet'], ['CodigoCurso'])

    expected_ds = spark_session.createDataFrame(
        [(12345, 'Juan Perez', 'Sociologia', 12345,1,80.0,1,4,'Int. a Sociologia'),(67890, 'Laura Chinchilla', 'Comunicacion', 67890,2,60.0,2,2,'Comunicacion'),(23456,'Luisa Lane Sanchez','Biologia',23456,3,85.0,3,4,'Matematica General'),(23987,'Eduardo Soto Jimenez','Filologia','23987',4,90.0,4,1,'Actividad Deportiva'),(78219,'Warner Sanchez Sanchez','Ing. Civil',78219,5,75.0,5,4,'Estatica'),(91234,'Marco Alvarado Rojas','Ing. Electrica',91234,6,70.0,6,4,'Circuicos en CC')])


    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()
