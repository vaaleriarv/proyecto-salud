import pandas as pd
import requests
import ssl
from io import BytesIO
import io   
import sqlite3
import zipfile
import gzip
import os
from functools import reduce

# --- Crear carpetas de data ---
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

for name, url in nhanes_urls.items():
    try:
        print(f"\nDescargando NHANES: {name}...")
        response = requests.get(url)
        response.raise_for_status()
        path = f"data_xpt/{name}.xpt"
        with open(path, "wb") as f:
            f.write(response.content)

        # Leer XPT y guardar en df
        df = pd.read_sas(path, format='xport')
        df.to_sql(name, conn, if_exists="replace", index=False)
        print(f"'{name}' guardado: {df.shape[0]} filas × {df.shape[1]} columnas")
    except Exception as e:
        print(f"Error con {name}: {e}")


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

# --- FoodData Central - Solo tablas necesarias ---
fdc_url = "https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_foundation_food_csv_2025-04-24.zip"
try:
    print("Descargando FoodData Central...", end=" ")
    resp = requests.get(fdc_url)
    resp.raise_for_status()
    path_zip = "data_csv/fooddata.zip"
    with open(path_zip, "wb") as f:
        f.write(resp.content)
    
    print("OK")
    print("Extrayendo archivos...", end=" ")
    with zipfile.ZipFile(path_zip, "r") as z:
        z.extractall("data_csv/fooddata")
    print("OK")
    
    # Solo cargar tablas principales
    tablas_fdc_principales = [
        'food.csv',
        'nutrient.csv', 
        'food_nutrient.csv',
        'food_category.csv',
        'food_portion.csv'
    ]
    
    import glob
    csv_path = "data_csv/fooddata/FoodData_Central_foundation_food_csv_2025-04-24"
    
    for tabla_nombre in tablas_fdc_principales:
        tabla_path = os.path.join(csv_path, tabla_nombre)
        if os.path.exists(tabla_path):
            table_name = tabla_nombre.replace('.csv', '').upper()
            print(f"Cargando {tabla_nombre}...", end=" ")
            df_fdc = pd.read_csv(tabla_path, low_memory=False)
            df_fdc.to_sql(f"FDC_{table_name}", conn, if_exists="replace", index=False)
            print(f"OK - {df_fdc.shape[0]} filas x {df_fdc.shape[1]} columnas")
        else:
            print(f"Advertencia: {tabla_nombre} no encontrado")
    
except Exception as e:
    print(f"ERROR: {e}")

# -- ODEPA 2025 --  

url_csv = "https://datos.odepa.gob.cl/dataset/c3ca8246-3d84-4145-9e34-525b0ba95859/resource/7f8f1255-a13b-4233-aad0-631054a8a025/download/precio_consumidor_publico_2025.csv"

try:
    print("Descargando ODEPA (CSV completo)...")
    response = requests.get(url_csv, verify=False)  
    response.raise_for_status()
    
    path_csv = "data_xpt/precio_consumidor_publico_2025.csv"
    with open(path_csv, "wb") as f:
        f.write(response.content)

    df_odepa = pd.read_csv(path_csv, encoding="utf-8")
    print(f"ODEPA cargado correctamente: {df_odepa.shape[0]} filas × {df_odepa.shape[1]} columnas")

    df_odepa.to_sql("ODEPA_2025", conn, if_exists="replace", index=False)
    print(f"'ODEPA_2025' guardado en SQLite: {df_odepa.shape[0]} filas × {df_odepa.shape[1]} columnas")

except Exception as e:
    print(f"Error al descargar ODEPA: {e}")

conn.close()
print("\n ¡Todos los datasets guardados en 'pipeline.db'!")
