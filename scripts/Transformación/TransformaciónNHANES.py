import pandas as pd
import sqlite3
import numpy as np

conn = sqlite3.connect("pipeline.db")

# -----------------------------------
# DEMO_L
# -----------------------------------
df_demo = pd.read_sql("SELECT * FROM DEMO_L", conn)

print("Valores únicos en RIAGENDR antes del mapeo:")
print(df_demo["RIAGENDR"].unique())

df_demo["RIAGENDR"] = pd.to_numeric(df_demo["RIAGENDR"], errors="coerce")

# Mapear sexo
sex_map = {1: "Hombre", 2: "Mujer"}
df_demo["RIAGENDR"] = df_demo["RIAGENDR"].map(sex_map)

# Crear rangos de edad
bins = list(range(0, 101, 5))
labels = [f"{b}-{b+4}" for b in bins[:-1]]
df_demo["edad_rango"] = pd.cut(df_demo["RIDAGEYR"], bins=bins, labels=labels, right=False)

# Mapear educación
educ_map = {
    1: 'No completó secundaria',
    2: 'Graduado de secundaria',
    3: 'Asistió a universidad o escuela técnica',
    4: 'Graduado de universidad o escuela técnica',
    9: 'No sabe / Falta de información'
}
df_demo["educacion"] = df_demo["DMDEDUC2"].apply(lambda x: educ_map.get(int(x), "Desconocido") if pd.notna(x) else np.nan)

# Mapear ingreso familiar
df_demo["ingreso"] = pd.cut(
    df_demo["INDFMPIR"],
    bins=[0,1,2,3,4,5,6,7,8,9,10,20,100],
    labels=["<1","1-2","2-3","3-4","4-5","5-6","6-7","7-8","8-9","9-10","10-20",">20"]
)

# Guardar DEMO limpio
df_demo.to_sql("DEMO_L", conn, if_exists="replace", index=False)

print("\nPrimeras filas DEMO después del mapeo de sexo:")
print(df_demo[["SEQN", "RIAGENDR"]].head())

conn.close()
