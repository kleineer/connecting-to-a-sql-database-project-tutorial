import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables

# 1) Connect to the database with SQLAlchemy

# 2) Create the tables

# 3) Insert data

# 4) Use Pandas to read and display a table


import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# Cargar variables de entorno
load_dotenv()

# Variable global para el motor de la base de datos
engine = None

def connect():
    global engine
    try:
        # AÑADIR sslmode=require A LA CADENA DE CONEXIÓN
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode=require"
        print("Iniciando la conexión a la base de datos...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        
        with engine.connect() as connection:
            print("Conectado exitosamente a la base de datos!")
        return engine
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# Función para ejecutar scripts SQL desde archivos
def execute_sql_file(filepath, engine_obj):
    try:
        with open(filepath, 'r') as f:
            sql_script = f.read()
        
        with engine_obj.connect() as connection:
            # Ejecutar el script SQL
            connection.execute(text(sql_script))
            # No es necesario connection.commit() con isolation_level="AUTOCOMMIT"
        print(f"Script SQL '{filepath}' ejecutado exitosamente.")
    except Exception as e:
        print(f"Error al ejecutar script SQL '{filepath}': {e}")

# --- Lógica principal de app.py ---
if __name__ == "__main__":
    db_engine = connect()

    if db_engine:
        # Eliminar tablas existentes antes de crearlas
        print("\nEliminando tablas existentes (si las hay)...")
        drop_sql_path = './src/sql/drop.sql'
        # Usamos un bloque try-except para drop.sql porque las tablas pueden no existir la primera vez
        try:
            execute_sql_file(drop_sql_path, db_engine)
        except Exception as e:
            print(f"Advertencia al eliminar tablas (posiblemente no existían): {e}")


        print("\nCreando tablas...")
        create_sql_path = './src/sql/create.sql'
        execute_sql_file(create_sql_path, db_engine)

        print("\nInsertando datos...")
        insert_sql_path = './src/sql/insert.sql'
        execute_sql_file(insert_sql_path, db_engine)

        print("\nLeyendo datos de una tabla con Pandas:")
        try:
            # Se ha reemplazado 'tu_nombre_de_tabla_aqui' por 'books'
            # Puedes elegir 'publishers', 'authors', o 'book_authors' si lo prefieres
            df_table = pd.read_sql("SELECT * FROM books LIMIT 5", db_engine)
            print(df_table)
        except Exception as e:
            print(f"Error al leer la tabla con Pandas: {e}")
    else:
        print("No se pudo establecer la conexión a la base de datos. El script finalizó.")