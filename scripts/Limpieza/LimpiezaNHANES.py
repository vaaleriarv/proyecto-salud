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
try:
    # HDL_L
    df_hdl = pd.read_sql("SELECT * FROM HDL_L", conn)
    df_hdl["LBXHDL_LIMPIO"] = pd.to_numeric(df_hdl["LBXHDL"], errors="coerce")
    df_hdl.loc[(df_hdl["LBXHDL_LIMPIO"] < 10) | (df_hdl["LBXHDL_LIMPIO"] > 200), "LBXHDL_LIMPIO"] = np.nan
    df_hdl.to_sql("HDL_L_LIMPIO", conn, if_exists="replace", index=False)

    # TRIGLY_L
    df_trig = pd.read_sql("SELECT * FROM TRIGLY_L", conn)
    df_trig["LBXTRIG_LIMPIO"] = pd.to_numeric(df_trig["LBXTRIG"], errors="coerce")
    df_trig.loc[(df_trig["LBXTRIG_LIMPIO"] < 10) | (df_trig["LBXTRIG_LIMPIO"] > 1000), "LBXTRIG_LIMPIO"] = np.nan
    df_trig.to_sql("TRIGLY_L_LIMPIO", conn, if_exists="replace", index=False)

    # TCHOL_L
    df_tchol = pd.read_sql("SELECT * FROM TCHOL_L", conn)
    df_tchol["LBXTCHOL_LIMPIO"] = pd.to_numeric(df_tchol["LBXTCHOL"], errors="coerce")
    df_tchol.loc[(df_tchol["LBXTCHOL_LIMPIO"] < 50) | (df_tchol["LBXTCHOL_LIMPIO"] > 500), "LBXTCHOL_LIMPIO"] = np.nan
    df_tchol.to_sql("TCHOL_L_LIMPIO", conn, if_exists="replace", index=False)

    # INS_L
    df_ins = pd.read_sql("SELECT * FROM INS_L", conn)
    df_ins["LBXINS_LIMPIO"] = pd.to_numeric(df_ins["LBXINS"], errors="coerce")
    df_ins.loc[(df_ins["LBXINS_LIMPIO"] < 1) | (df_ins["LBXINS_LIMPIO"] > 1000), "LBXINS_LIMPIO"] = np.nan
    df_ins.to_sql("INS_L_LIMPIO", conn, if_exists="replace", index=False)

    # BIOPRO_L
    df_biopro = pd.read_sql("SELECT * FROM BIOPRO_L", conn)
    df_biopro["LBDBIO_LIMPIO"] = pd.to_numeric(df_biopro["LBDBIO"], errors="coerce")
    df_biopro.loc[(df_biopro["LBDBIO_LIMPIO"] < 1) | (df_biopro["LBDBIO_LIMPIO"] > 1000), "LBDBIO_LIMPIO"] = np.nan
    df_biopro.to_sql("BIOPRO_L_LIMPIO", conn, if_exists="replace", index=False)

    # FOLATE_L
    df_folate = pd.read_sql("SELECT * FROM FOLATE_L", conn)
    df_folate["LBXFA_LIMPIO"] = pd.to_numeric(df_folate["LBXFA"], errors="coerce")
    df_folate.loc[(df_folate["LBXFA_LIMPIO"] < 1) | (df_folate["LBXFA_LIMPIO"] > 200), "LBXFA_LIMPIO"] = np.nan
    df_folate.to_sql("FOLATE_L_LIMPIO", conn, if_exists="replace", index=False)

    # FASTQX_L
    df_fastq = pd.read_sql("SELECT * FROM FASTQX_L", conn)
    df_fastq["LBXFAST_LIMPIO"] = pd.to_numeric(df_fastq["LBXFAST"], errors="coerce")
    df_fastq.loc[(df_fastq["LBXFAST_LIMPIO"] < 50) | (df_fastq["LBXFAST_LIMPIO"] > 1000), "LBXFAST_LIMPIO"] = np.nan
    df_fastq.to_sql("FASTQX_L_LIMPIO", conn, if_exists="replace", index=False)

except Exception as e:
    print("Error en limpieza de perfil lipídico/bioquímica:", e)


conn.close()
