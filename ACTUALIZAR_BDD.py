import pandas as pd
import pyodbc
from tqdm import tqdm


def actualizar_base_de_datos(csv_file, server, database, username, password, table):
    # Leer los datos del archivo CSV y forzar las columnas a cadenas (str)
    data = pd.read_csv(csv_file, dtype={'BODEGA': str, 'CODIGO': str})

    # Conectar a la base de datos
    conn = pyodbc.connect(
        f"DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    cursor = conn.cursor()

    # Obtener el número total de filas en el DataFrame para la barra de progreso
    total_rows = len(data)

    # Crear una barra de progreso
    progress_bar = tqdm(total=total_rows, desc="Progreso")

    # Iterar a través de los datos del CSV y actualizar la base de datos
    for index, row in data.iterrows():
        bodega = row['BODEGA']
        codigo = row['CODIGO']
        new_max = row['NEW_MAX']

        # Ajustar la consulta SQL para tratar bodega y codigo como cadenas
        query = f"""
        UPDATE {table}
        SET sm_nuevoMaximo = {new_max}
        WHERE idOficina = '{bodega}' AND codigo = '{codigo}'
        """

        cursor.execute(query)
        conn.commit()

        # Actualizar la barra de progreso
        progress_bar.update(1)

    # Cerrar la barra de progreso y la conexión a la base de datos
    progress_bar.close()
    conn.close()


# Llamar a la función para actualizar la base de datos
actualizar_base_de_datos(
    csv_file='SIEMBRA_20230904_02.csv',
    server='192.168.147.32',
    database='EasyGestionEmpresarial',
    username='sa',
    password='sqlfarma',
    table='[dbo].[simulador2_ALL_Productos]'
)
