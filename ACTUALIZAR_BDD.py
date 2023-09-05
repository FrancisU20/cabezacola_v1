import time
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import time
from ENVIAR_MENSAJE import varios_destinatarios

def actualizar_base_de_datos(csv_file, server, database, username, password):
    # Leer los datos del archivo CSV y forzar las columnas a cadenas (str)
    
    varios_destinatarios('*Proceso de actualización nuevos máximos iniciado:*')
    time.sleep(10)
    
    print("Leyendo datos del archivo CSV...")
    varios_destinatarios('*Paso 1 de 5:* Leyendo datos del archivo CSV...')
    time.sleep(10)
    data = pd.read_csv(csv_file, dtype={"BODEGA": str, "CODIGO": str})

    # Conectar a la base de datos
    print("Conectando a la base de datos...")
    varios_destinatarios('*Paso 2 de 5:* Conectando a la base de datos...')
    time.sleep(10)
    conn = pyodbc.connect(
        f"DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    cursor = conn.cursor()

    # Crear una cadena de conexión compatible con SQLAlchemy
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

    # Crear el motor SQLAlchemy con la cadena de conexión
    engine = create_engine(connection_string)

    print("Conexión exitosa")

    # Luego, usa el motor 'engine' para cargar los datos desde el DataFrame a la tabla
    print("Cargando datos a la base de datos...")
    varios_destinatarios('*Paso 3 de 5:* Cargando datos a la base de datos...')
    data.to_sql(
        name="tbl_TemporalMaximos",
        con=engine,
        schema="abst",
        if_exists="replace",
        index=False,
    )

    try:
        # Ajustar la consulta SQL para tratar bodega y código como cadenas
        query = f"""
        exec abst.ActualizarNuevosMáximos
        """

        cursor.execute(query)
        conn.commit()
        print("Datos actualizados exitosamente en tabla simulador")
        varios_destinatarios('*Paso 4 de 5:* Datos actualizados exitosamente en tabla simulador')
        time.sleep(10)

        # Borrar la tabla abst.tbl_TemporalMaximos
        drop_query = "DROP TABLE abst.tbl_TemporalMaximos"
        cursor.execute(drop_query)
        conn.commit()
        print("Tabla temporal eliminada exitosamente")
        varios_destinatarios('*Paso 5 de 5:* Tabla temporal eliminada exitosamente')
        time.sleep(10)

        print("Proceso finalizado exitosamente")
        varios_destinatarios('*Proceso finalizado exitosamente*')
    except Exception as e:
        print(f"Error al actualizar la base de datos: {e}")
        varios_destinatarios(f"Error al actualizar la base de datos: {e}")
    finally:
        # Cerrar la conexión a la base de datos
        conn.close()

# Función para mostrar el tiempo transcurrido
def mostrar_tiempo_transcurrido(start_time):
    elapsed_time = time.time() - start_time
    print(f"Tiempo transcurrido: {elapsed_time:.2f} segundos")

# Llamar a la función para actualizar la base de datos
if __name__ == "__main__":
    start_time = time.time()  # Capturar el tiempo de inicio
    actualizar_base_de_datos(
        csv_file="SIEMBRA_20230904_02.csv",
        server="192.168.147.32",
        database="EasyGestionEmpresarial",
        username="sa",
        password="sqlfarma",
    )
    mostrar_tiempo_transcurrido(start_time)  # Mostrar el tiempo transcurrido
