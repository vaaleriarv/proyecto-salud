import pandas as pd
import sqlite3
import numpy as np
from functools import reduce

conn = sqlite3.connect("pipeline.db")

tablas_nhanes = [
    "ALB_CR_L", "AGP_L", "HDL_L", "TRIGLY_L", "TCHOL_L", "CBC_L",
    "FASTQX_L", "FERTIN_L", "FOLATE_L", "GHB_L", "HEPA_L", "HEPB_S_L",
    "HSCRP_L", "INS_L", "PBCD_L", "IHGEM_L", "GLU_L", "FOLFMS_L",
    "TST_L", "BIOPRO_L", "TFR_L", "UCPREG_L", "VID_L", "VOCWB_L",
    "BMX_L", "DEMO_L", "DR1TOT_L", "DR2TOT_L"
]

dfs = {}
for nombre in tablas_nhanes:
    try:
        df_temp = pd.read_sql(f"SELECT * FROM {nombre}", conn)
        if "SEQN" not in df_temp.columns:
            continue
        df_temp = df_temp.rename(columns={col: f"{nombre}_{col}" for col in df_temp.columns if col != "SEQN"})
        dfs[nombre] = df_temp
    except:
        continue

df_final = reduce(lambda left, right: pd.merge(left, right, on="SEQN", how="outer"), dfs.values())

df_final.to_sql("NHANESLIMPIO", conn, if_exists="replace", index=False)

conn.commit()

tablas = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
for t in tablas:
    print("-", t[0])

#--- MAPEO DE DEMO_L ---
demo_mappings = {
    "DMDBORN4": {1:"Nacido en EE.UU.", 2:"Otros países", 77:"Se negó a responder", 99:"No sabe"},
    "DMDEDUC2": {1:"Menos de 9º grado",2:"9-11º grado",3:"Graduado secundaria",4:"Alguna universidad/AA",5:"Graduado universitario",7:"Se negó a responder",9:"No sabe"},
    "DMDHHSIZ": {1:"1 persona",2:"2 personas",3:"3 personas",4:"4 personas",5:"5 personas",6:"6 personas",7:"7 o más personas"},
    "DMDHRAGZ": {1:"<20 años",2:"20-39 años",3:"40-59 años",4:"60+ años"},
    "DMDHRGND": {1:"Hombre",2:"Mujer"},
    "DMDMARTZ": {1:"Casado/Vive con pareja",2:"Viudo/Divorciado/Separado",3:"Nunca se casó",77:"Se negó a responder",99:"No sabe"}
}

for col, mapping in demo_mappings.items():
    if col in df_final.columns:
        df_final[col] = df_final[col].map(mapping)

# --- LIMPIEZA DR1TOT_L y DR2TOT_L (numéricas) ---
dr_columns = [col for col in df_final.columns if "DR1TOT_L" in col or "DR2TOT_L" in col]
df_final[dr_columns] = df_final[dr_columns].replace({'.': np.nan})
df_final[dr_columns] = df_final[dr_columns].apply(pd.to_numeric, errors='coerce')

# --- GUARDAR ---
df_final.to_sql("NHANES_LIMPIO", conn, if_exists="replace", index=False)

# --- Revisar tablas ---
tablas = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
print("Tablas disponibles:")
for t in tablas:
    print("-", t[0])

# --- Hacer backup de la tabla original ---
df_original = pd.read_sql("SELECT * FROM NHANES_2021", conn)
df_original.to_sql("NHANES_2021_backup", conn, if_exists="replace", index=False)
print("Backup de NHANES_2021 creado como NHANES_2021_backup")

# --- Sobrescribir la tabla original con los datos limpios ---
df_final.to_sql("NHANES_2021", conn, if_exists="replace", index=False)
print("Tabla NHANES_2021 sobrescrita con la versión limpia")

cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS NHANES_LIMPIO;")
conn.commit()

conn.close()
