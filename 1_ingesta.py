import pandas as pd
import sqlite3

conn = sqlite3.connect("pipeline_proyecto.db")

# Cargar cada dataset
df_brfss = pd.read_sas("LLCP2024.xpt")
df_nhanes = pd.read_csv("NHANES2024.csv")
df_openfood = pd.read_csv("OpenFood.csv")
df_precios = pd.read_csv("Precios.csv")

# Guardar en SQLite
df_brfss.to_sql("brfss", conn, if_exists="replace", index=False)
df_nhanes.to_sql("nhanes", conn, if_exists="replace", index=False)
df_openfood.to_sql("openfood", conn, if_exists="replace", index=False)
df_precios.to_sql("precios", conn, if_exists="replace", index=False)

print("✅ Ingesta completa: datasets guardados en SQLite.")
