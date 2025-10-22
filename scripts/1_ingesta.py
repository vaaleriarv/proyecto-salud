import pandas as pd
import requests
from io import BytesIO
import io   
import sqlite3
import zipfile
import gzip
import os
from functools import reduce



# --- Crear carpetas temporales ---
os.makedirs("data_xpt", exist_ok=True)
os.makedirs("data_csv", exist_ok=True)

# --- Conectar SQLite ---
conn = sqlite3.connect("pipeline.db")

# --- NHANES 2021 ---
nhanes_urls = {
    "ALB_CR_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/ALB_CR_L.xpt",
    "AGP_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/AGP_L.xpt",
    "HDL_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/HDL_L.xpt",
    "TRIGLY_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/TRIGLY_L.xpt",
    "TCHOL_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/TCHOL_L.xpt",
    "CBC_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/CBC_L.xpt",
    "FASTQX_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/FASTQX_L.xpt",
    "FERTIN_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/FERTIN_L.xpt",
    "FOLATE_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/FOLATE_L.xpt",
    "GHB_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/GHB_L.xpt",
    "HEPA_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/HEPA_L.xpt",
    "HEPB_S_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/HEPB_S_L.xpt",
    "HSCRP_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/HSCRP_L.xpt",
    "INS_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/INS_L.xpt",
    "PBCD_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/PBCD_L.xpt",
    "IHGEM_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/IHGEM_L.xpt",
    "GLU_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/GLU_L.xpt",
    "FOLFMS_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/FOLFMS_L.xpt",
    "TST_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/TST_L.xpt",
    "BIOPRO_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/BIOPRO_L.xpt",
    "TFR_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/TFR_L.xpt",
    "UCPREG_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/UCPREG_L.xpt",
    "VID_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/VID_L.xpt",
    "VOCWB_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/VOCWB_L.xpt",
    "BMX_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/BMX_L.xpt",
    "DEMO_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/DEMO_L.xpt",
    "DR1TOT_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/DR1TOT_L.xpt",
    "DR2TOT_L": "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/DR2TOT_L.xpt"
}

# Descarga y guarda tablas individuales
for name, url in nhanes_urls.items():
    try:
        print(f"\nDescargando NHANES: {name}...")
        response = requests.get(url)
        response.raise_for_status()
        path = f"data_xpt/{name}.xpt"
        with open(path, "wb") as f:
            f.write(response.content)
        df = pd.read_sas(path, format="xport")
        df.to_sql(name, conn, if_exists="replace", index=False)
        print(f"'{name}' guardado: {df.shape[0]} filas × {df.shape[1]} columnas")
    except Exception as e:
        print(f"Error con {name}: {e}")

# --- CREAR TABLA COMBINADA NHANES_2021 ---
tablas_nhanes = list(nhanes_urls.keys())
dfs = {}

for nombre in tablas_nhanes:
    df_temp = pd.read_sql(f"SELECT * FROM {nombre}", conn)
    if "SEQN" not in df_temp.columns:
        continue
    df_temp = df_temp.rename(columns={col: f"{nombre}_{col}" for col in df_temp.columns if col != "SEQN"})
    dfs[nombre] = df_temp

df_nhanes = reduce(lambda left, right: pd.merge(left, right, on="SEQN", how="outer"), dfs.values())
df_nhanes.to_sql("NHANES_2021", conn, if_exists="replace", index=False)
print("\nTabla combinada 'NHANES_2021' creada y guardada.")

# --- BRFSS 2024 ---
brfss_url = "https://www.cdc.gov/brfss/annual_data/2024/files/LLCP2024XPT.zip"
try:
    print("\nDescargando BRFSS 2024...")
    resp = requests.get(brfss_url)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        print("Archivos en ZIP:", z.namelist())
        xpt_files = [f for f in z.namelist() if ".xpt" in f.lower()]

        if not xpt_files:
            raise Exception("No se encontró archivo .xpt en el ZIP")

        with z.open(xpt_files[0]) as f:
            df_brfss = pd.read_sas(f, format="xport")

            # Definir las columnas que te interesan
            columnas_relevantes = [
                '_SEQNO' ,'_STATE', 'MARITAL', '_CHLDCNT', '_INCOMG1', '_AGE_G', '_SEX', '_RACE',
                '_URBSTAT', '_METSTAT', '_EDUCAG', 'MEDCOST1', 'CHECKUP1', '_HLTHPL2',
                'PDIABTS1', 'DIABETE4', 'DIABAGE4', 'DIABTYPE', 'PREDIAB2', 'EXERANY2',
                '_TOTINDA', 'WEIGHT2', 'WTKG3', 'HEIGHT3', '_BMI5', '_BMI5CAT', '_RFBMI5',
                'SMOKDAY2', 'LCSFIRST', 'LCSNUMCG', '_SMOKER3', 'LCSLAST_', 'LCSNUMC_',
                '_LCSSMKG', '_LCSYSMK', 'ALCDAY4', 'AVEDRNK4', 'DRNK3GE5', '_DRNKWK3',
                '_RFDRHV9', 'MARIJAN1', 'SSBFRUT3'
            ]

            columnas_existentes = [c for c in columnas_relevantes if c in df_brfss.columns]
            df_brfss = df_brfss[columnas_existentes]

            df_brfss.to_sql("BRFSS_2024", conn, if_exists="replace", index=False)
            print(f"'BRFSS_2024' guardado (solo columnas seleccionadas): {df_brfss.shape[0]} filas × {df_brfss.shape[1]} columnas")

except Exception as e:
    print(f"Error con BRFSS: {e}")

# --- FoodData Central ---
fdc_url = "https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_foundation_food_csv_2025-04-24.zip"
try:
    print("\n Descargando FoodData Central...")
    resp = requests.get(fdc_url)
    resp.raise_for_status()
    path_zip = "data_csv/fooddata.zip"
    with open(path_zip, "wb") as f:
        f.write(resp.content)
    with zipfile.ZipFile(path_zip, "r") as z:
        z.extractall("data_csv/fooddata")
    # Leer todos los CSV dentro de la carpeta
    import glob
    csv_files = glob.glob("data_csv/fooddata/*.csv")
    for csv_file in csv_files:
        table_name = os.path.splitext(os.path.basename(csv_file))[0]
        df_fdc = pd.read_csv(csv_file)
        df_fdc.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f" '{table_name}' guardado: {df_fdc.shape[0]} filas × {df_fdc.shape[1]} columnas")
except Exception as e:
    print(f" Error con FoodData Central: {e}")

    # --- ODEPA Precios al Consumidor ---
try:
    print("\n Descargando ODEPA precios al consumidor...")
    url = "https://datos.odepa.gob.cl/api/3/action/datastore_search"
    params = {
        "resource_id": "7f8f1255-a13b-4233-aad0-631054a8a025",
        "limit": 50000  # ajusta según necesidad
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    records = data["result"]["records"]
    df_odepa = pd.DataFrame(records)
    df_odepa.to_sql("ODEPA_Precios", conn, if_exists="replace", index=False)
    print(f" 'ODEPA_Precios' guardado: {df_odepa.shape[0]} filas × {df_odepa.shape[1]} columnas")
except Exception as e:
    print(f" Error con ODEPA: {e}")

# --- Cerrar SQLite ---
conn.close()
print("\n ¡Todos los datasets guardados en 'pipeline.db'!")
