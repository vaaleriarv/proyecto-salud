import pandas as pd
import sqlite3
import numpy as np

conn = sqlite3.connect("pipeline.db")

# --- BMX_L: calcular IMC y clasificar ---
df_bmx = pd.read_sql("SELECT * FROM BMX_L", conn)

df_bmx['IMC_calc'] = df_bmx['BMXWT'] / ((df_bmx['BMXHT']/100)**2)

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

df_bmx = df_bmx[['SEQN', 'BMXWT', 'BMXHT', 'BMDBMIC']]

df_bmx.to_sql("BMX_L", conn, if_exists="replace", index=False)

print("Conteo categorías IMC:")
print(df_bmx['BMDBMIC'].value_counts(dropna=False))


# --- GLU_L: crear columna GLURANGOS ---
# Columna con rangos de glucosa en ayunas
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

print(df_bmx['BMDBMIC'].value_counts(dropna=False))
print(df_glu[["LBXGLU", "GLURANGOS"]].head())

conn.close()
