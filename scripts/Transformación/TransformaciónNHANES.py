import pandas as pd
import sqlite3
import numpy as np


conn = sqlite3.connect("pipeline.db")


df_demo = pd.read_sql("SELECT * FROM DEMO_L", conn)

# Mapear sexo
sex_map = {1: 'Hombre', 2: 'Mujer'}
df_demo['RIAGENDR'] = df_demo['RIAGENDR'].map(sex_map)

# Crear rangos de edad (cada 5 años)
bins = list(range(0, 101, 5))  # 0-4, 5-9, ..., 95-100
labels = [f"{b}-{b+4}" for b in bins[:-1]]
df_demo['edad_rango'] = pd.cut(df_demo['RIDAGEYR'], bins=bins, labels=labels, right=False)

# Mapear educación
educ_map = {
    1: 'No completó secundaria',
    2: 'Graduado de secundaria',
    3: 'Asistió a universidad o escuela técnica',
    4: 'Graduado de universidad o escuela técnica',
    9: 'No sabe / Falta de información'
}
df['educacion'] = df['DMDEDUC2'].apply(lambda x: educ_map.get(int(x), "Desconocido"))

# Mapear ingreso familiar (INDFMPIR)
df_demo['ingreso'] = pd.cut(df_demo['INDFMPIR'],
                             bins=[0,1,2,3,4,5,6,7,8,9,10,20,100],
                             labels=["<1","1-2","2-3","3-4","4-5","5-6","6-7","7-8","8-9","9-10","10-20",">20"])

# Guardar DEMO_L limpio
df_demo.to_sql("DEMO_L", conn, if_exists="replace", index=False)

# -----------------------------------
# 3️⃣ Transformación BMX_L (IMC)
# -----------------------------------
df_bmx = pd.read_sql("SELECT * FROM BMX_L", conn)

# Calcular IMC
df_bmx['IMC_calc'] = df_bmx['BMXWT'] / ((df_bmx['BMXHT']/100)**2)

# Clasificar IMC
def clasificar_imc(imc):
    if pd.isna(imc):
        return np.nan
    elif imc < 18.5:
        return "Bajo peso"
    elif imc < 25:
        return "Normal"
    elif imc < 30:
        return "Sobrepeso"
    else:
        return "Obesidad"

df_bmx['BMDBMIC'] = df_bmx['IMC_calc'].apply(clasificar_imc)

df_bmx = df_bmx[['SEQN','BMXWT','BMXHT','BMDBMIC']]
df_bmx.to_sql("BMX_L", conn, if_exists="replace", index=False)

# -----------------------------------
# 4️⃣ Transformación GLU_L (glucosa)
# -----------------------------------
df_glu = pd.read_sql("SELECT * FROM GLU_L", conn)

def categorizar_glucosa(x):
    if pd.isna(x):
        return np.nan
    elif 70 <= x <= 99:
        return "Normal"
    elif 100 <= x <= 125:
        return "Prediabetes"
    elif x >= 126:
        return "Diabetes"
    else:
        return np.nan

df_glu["GLURANGOS"] = df_glu["LBXGLU"].apply(categorizar_glucosa)
df_glu = df_glu.dropna(subset=['GLURANGOS'])
df_glu.to_sql("GLU_L", conn, if_exists="replace", index=False)

# -----------------------------------
# 5️⃣ Opcional: unir tablas DEMO + BMX + GLU
# -----------------------------------
df_final = df_demo[['SEQN','RIAGENDR','edad_rango','educacion','ingreso']].merge(
    df_bmx[['SEQN','BMDBMIC']], on='SEQN', how='left'
).merge(
    df_glu[['SEQN','GLURANGOS']], on='SEQN', how='left'
)

df_final.to_sql("NHANES_LIMPIO", conn, if_exists="replace", index=False)

# -----------------------------------
# 6️⃣ Revisar conteos y primeras filas
# -----------------------------------
print("Conteo categorías IMC:")
print(df_bmx['BMDBMIC'].value_counts(dropna=False))

print("\nConteo rangos de glucosa:")
print(df_glu['GLURANGOS'].value_counts(dropna=False))

print("\nPrimeras filas del dataset final:")
print(df_final.head())

conn.close()
