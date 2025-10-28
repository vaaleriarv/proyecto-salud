import sqlite3
import pandas as pd
from sqlalchemy import create_engine

#Migration de SQLite a PostgreSQL para pasarlo a Power BI

#Configuración PostgreSQL
usuario = "postgres"
clave = "1234"  
host = "localhost"
puerto = "5432"
base_datos = "pipeline" 

#Conexión SQLite
sqlite_conn = sqlite3.connect("pipeline.db")
tablas = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", sqlite_conn)

#Conexión PostgreSQL 
postgres_url = f"postgresql+psycopg2://{usuario}:{clave}@{host}:{puerto}/{base_datos}"
engine = create_engine(postgres_url)

#Migrar todas las tablas del pipeline.db a PostgreSQL 
for t in tablas["name"]:
    print(f"Migrando tabla: {t}")
    df = pd.read_sql(f"SELECT * FROM {t}", sqlite_conn)
    df.to_sql(t, engine, if_exists="replace", index=False)
    print(f" Tabla {t} migrada correctamente")

sqlite_conn.close()
engine.dispose()
print("\n Migración completa: todas las tablas ahora están en PostgreSQL")
