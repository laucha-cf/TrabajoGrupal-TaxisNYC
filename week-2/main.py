#-- Pipeline --#
if __name__ == '__main__':
    print('Extract...')
    exec(open('Extract.py').read())
    
    print('Data Cleaning...')
    exec(open('DataCleaning.py').read())
    
    print('DDL 1...')
    exec(open('ddl1_tables.py').read())
    
    print('Segmentación de Tablas...')
    exec(open('Segmentacion_Tablas.py').read())
    
    print('Carga inicial...')
    exec(open('Carga_inicial.py').read())