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
except Exception as e:
    print(f"Error en limpieza DEMO_L:", e)
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
    df_dr2.to_sql("DR2TOT_L_LIMPIO", conn, if_exists="replace", index=False)
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
    df_bmx.to_sql("BMX_L_LIMPIO", conn, if_exists="replace", index=False)
except:
    pass

# --- LIMPIEZA GLU_L ---
try:
    df_glu = pd.read_sql("SELECT * FROM GLU_L", conn)
    
    # Columnas limpias
    df_glu["LBXGLU_LIMPIO"] = pd.to_numeric(df_glu["LBXGLU"], errors="coerce")
    df_glu["LBDGLUSI_LIMPIO"] = pd.to_numeric(df_glu["LBDGLUSI"], errors="coerce")
    
    # Filtrar outliers
    df_glu.loc[(df_glu["LBXGLU_LIMPIO"] < 40) | (df_glu["LBXGLU_LIMPIO"] > 500), "LBXGLU_LIMPIO"] = np.nan
    df_glu.loc[(df_glu["LBDGLUSI_LIMPIO"] < 1) | (df_glu["LBDGLUSI_LIMPIO"] > 50), "LBDGLUSI_LIMPIO"] = np.nan
    
    df_glu.to_sql("GLU_L_LIMPIO", conn, if_exists="replace", index=False)
except:
    pass

# --- LIMPIEZA PERFIL LIPÍDICO Y BIOQUÍMICA ---
print("\nLimpieza de perfil lipídico y bioquímica")

tablas_perfil = {
    "HDL_L": {"nombre_col": ["LBDHDD", "LBXHDL"], "rango": (10, 200)},
    "TRIGLY_L": {"nombre_col": ["LBDTRSI", "LBXTLG"], "rango": (10, 1000)},  # cambiamos LBXTRIG -> LBXTLG
    "TCHOL_L": {"nombre_col": ["LBXTC"], "rango": (50, 500)},
    "INS_L": {"nombre_col": ["LBXIN", "LBDINSI"], "rango": (1, 1000)},
    "BIOPRO_L": {"nombre_col": ["LBXSUA"], "rango": (1, 1000)},  # elige la proteína que te interesa
    "FOLATE_L": {"nombre_col": ["LBDRFO"], "rango": (1, 200)},
    "FASTQX_L": {"nombre_col": ["PHAFSTHR"], "rango": (50, 1000)}  # ejemplo si quieres usar PHA fasting
}

for tabla, info in tablas_perfil.items():
    try:
        print(f"\nProcesando tabla '{tabla}'...")
        df = pd.read_sql(f"SELECT * FROM {tabla}", conn)
        print(f"  Columnas disponibles: {df.columns.tolist()}")

        # Buscar la columna válida
        col_valida = None
        for col in info["nombre_col"]:
            if col in df.columns:
                col_valida = col
                break
        if col_valida is None:
            print(f"  No se encontró columna esperada en {tabla}, se omite")
            continue

        # Limpiar y convertir a numérico
        df[f"{tabla}_LIMPIO"] = pd.to_numeric(df[col_valida], errors="coerce")
        df.loc[
            (df[f"{tabla}_LIMPIO"] < info["rango"][0]) | (df[f"{tabla}_LIMPIO"] > info["rango"][1]),
            f"{tabla}_LIMPIO"
        ] = np.nan

        print(f"  Valores válidos: {df[f'{tabla}_LIMPIO'].notna().sum():,}")
        print(f"  Valores inválidos / NaN: {df[f'{tabla}_LIMPIO'].isna().sum():,}")

        # Guardar tabla limpia
        df.to_sql(f"{tabla}_L_LIMPIO", conn, if_exists="replace", index=False)
        print(f"  ✔ Tabla '{tabla}_L_LIMPIO' creada correctamente")

    except Exception as e:
        print(f"  Error procesando {tabla}: {e}")


conn.close()
