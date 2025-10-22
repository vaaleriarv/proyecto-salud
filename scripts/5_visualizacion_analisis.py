import sqlite3
import pandas as pd

# Conectar a la base
conn = sqlite3.connect("pipeline.db")

# 1️⃣ Listar todas las tablas
tablas = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
print("📂 Tablas disponibles:")
print(tablas["name"].to_list())

# 2️⃣ Mostrar información de cada tabla
for tabla in tablas["name"]:
    # Contar filas
    total_filas = pd.read_sql(f"SELECT COUNT(*) as filas FROM {tabla};", conn)["filas"].iloc[0]
    # Contar columnas
    df_temp = pd.read_sql(f"SELECT * FROM {tabla} LIMIT 5;", conn)
    print(f"\n✅ {tabla}: {total_filas} filas × {len(df_temp.columns)} columnas")
    print(df_temp.head())  # solo primeras 5 filas

# Cerrar conexión
conn.close()
