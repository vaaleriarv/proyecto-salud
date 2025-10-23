import pandas as pd
import sqlite3
import numpy as np

conn = sqlite3.connect("pipeline.db")

# --- LIMPIEZA DEMO_L ---
try:
    df_demo = pd.read_sql("SELECT * FROM DEMO_L", conn)
    demo_mappings = {
        "DMDBORN4": {1:"Nacido en EE.UU.", 2:"Otros países", 77:"Se negó a responder", 99:"No sabe"},
        "DMDEDUC2": {1:"Menos de 9º grado",2:"9-11º grado",3:"Graduado secundaria",
                     4:"Alguna universidad/AA",5:"Graduado universitario",7:"Se negó a responder",9:"No sabe"},
        "DMDHHSIZ": {1:"1 persona",2:"2 personas",3:"3 personas",4:"4 personas",
                     5:"5 personas",6:"6 personas",7:"7 o más personas"},
        "DMDHRAGZ": {1:"<20 años",2:"20-39 años",3:"40-59 años",4:"60+ años"},
        "DMDHRGND": {1:"Hombre",2:"Mujer"},
        "DMDMARTZ": {1:"Casado/Vive con pareja",2:"Viudo/Divorciado/Separado",
                      3:"Nunca se casó",77:"Se negó a responder",99:"No sabe"}
    }
    for col, mapping in demo_mappings.items():
        if col in df_demo.columns:
            df_demo[col] = df_demo[col].map(mapping)
    df_demo.to_sql("DEMO_L_LIMPIO", conn, if_exists="replace", index=False)
except:
    pass

# --- LIMPIEZA DR1TOT_L ---
try:
    df_dr1 = pd.read_sql("SELECT * FROM DR1TOT_L", conn)
    for col in df_dr1.columns:
        if col != "SEQN":
            df_dr1[col] = pd.to_numeric(df_dr1[col].replace({'.': np.nan}), errors='coerce')
    df_dr1.to_sql("DR1TOT_L_LIMPIO", conn, if_exists="replace", index=False)
except:
    pass

# --- LIMPIEZA DR2TOT_L ---
try:
    df_dr2 = pd.read_sql("SELECT * FROM DR2TOT_L", conn)
    for col in df_dr2.columns:
        if col != "SEQN":
            df_dr2[col] = pd.to_numeric(df_dr2[col].replace({'.': np.nan}), errors='coerce')
    df_dr2.to_sql("DR2TOT_L", conn, if_exists="replace", index=False)
except:
    pass

# --- LIMPIEZA BMX_L ---
try:
    df_bmx = pd.read_sql("SELECT * FROM BMX_L", conn)
    columnas_a_conservar = ["SEQN", "BMXWT", "BMXHT", "BMDBMIC"]
    df_bmx = df_bmx[columnas_a_conservar]

    df_bmx[["BMXWT", "BMXHT"]] = df_bmx[["BMXWT", "BMXHT"]].apply(pd.to_numeric, errors='coerce')

    df_bmx = df_bmx[
        (df_bmx["BMXWT"] >= 20) & (df_bmx["BMXWT"] <= 300) &
        (df_bmx["BMXHT"] >= 120) & (df_bmx["BMXHT"] <= 220)
    ]

    df_bmx.to_sql("BMX_L", conn, if_exists="replace", index=False)
except:
    pass

tablas = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
for t in tablas:
    print("-", t[0])

conn.close()
