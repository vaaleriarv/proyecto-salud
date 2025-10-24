import pandas as pd
import numpy as np
import sqlite3
import sqlite3
conn = sqlite3.connect("pipeline.db")

# NHANES
nhanes_integrado = pd.read_sql("SELECT SEQN, LBXGH_LIMPIO AS HbA1c FROM GHB_L_LIMPIO", conn)

try:
    brfss_clean = pd.read_sql("SELECT * FROM BRFSS_2024_LIMPIO", conn)
except Exception:
    brfss_clean = None


# FDC (si existe)
fdc_clean = {}
try:
    fdc_clean['food'] = pd.read_sql("SELECT * FROM FDC_FOOD_LIMPIO", conn)
    fdc_clean['food_nutrient'] = pd.read_sql("SELECT * FROM FDC_FOOD_NUTRIENT_LIMPIO", conn)
except Exception:
    fdc_clean = None


## ======================================================
# 4.1. Consolidación NHANES - Dataset Integrado
# ======================================================
print("4.1. Consolidación NHANES - Dataset Integrado de Salud Metabólica\n")

# Iniciar con GHB_L
nhanes_integrado = pd.read_sql("SELECT SEQN, LBXGH_LIMPIO AS HbA1c FROM GHB_L_LIMPIO", conn)

# Función para agregar variables metabólicas desde tablas LIMPIO
def agregar_variable(tabla, col_origen, col_nuevo):
    df_temp = pd.read_sql(f"SELECT SEQN, {col_origen} FROM {tabla}_LIMPIO", conn)
    df_temp.rename(columns={col_origen: col_nuevo}, inplace=True)
    return df_temp

# Variables metabólicas
variables_metabolicas = [
    ('GLU_L', 'LBXGLU_LIMPIO', 'Glucosa'),
    ('INS_L', 'LBXIN_LIMPIO', 'Insulina'),
    ('HDL_L', 'LBDHDD_LIMPIO', 'HDL'),
    ('TCHOL_L', 'LBXTC_LIMPIO', 'Colesterol_Total'),
    ('TRIGLY_L', 'LBDTRSI_LIMPIO', 'Trigliceridos'),
    ('HSCRP_L', 'LBXHSCRP_LIMPIO', 'PCR') 
]

for tabla, col_origen, col_nuevo in variables_metabolicas:
    temp = agregar_variable(tabla, col_origen, col_nuevo)
    nhanes_integrado = nhanes_integrado.merge(temp, on='SEQN', how='left')

# Agregar variables DEMO_L
df_demo = pd.read_sql("SELECT SEQN, RIDAGEYR, RIAGENDR, DMDEDUC2, INDFMPIR FROM DEMO_L_LIMPIO", conn)

# Edad en rangos
bins = list(range(0, 101, 5))
labels = [f"{b}-{b+4}" for b in bins[:-1]]
df_demo['edad_rango'] = pd.cut(df_demo['RIDAGEYR'], bins=bins, labels=labels, right=False)
df_demo['sexo'] = df_demo['RIAGENDR']
df_demo['educacion'] = df_demo['DMDEDUC2']
df_demo['ingreso'] = pd.cut(
    df_demo['INDFMPIR'],
    bins=[0,1,2,3,4,5,6,7,8,9,10,20,100],
    labels=["<1","1-2","2-3","3-4","4-5","5-6","6-7","7-8","8-9","9-10","10-20",">20"]
)

nhanes_integrado = nhanes_integrado.merge(df_demo[['SEQN','edad_rango','sexo','educacion','ingreso']], on='SEQN', how='left')

# Guardar dataset NHANES integrado
nhanes_integrado.to_sql("NHANES_INTEGRADO", conn, if_exists="replace", index=False)
print(f"Dataset NHANES integrado creado: {nhanes_integrado.shape[0]:,} participantes x {nhanes_integrado.shape[1]} variables\n")


# ======================================================
# 4.2. Crear Variables Derivadas NHANES
# ======================================================
print("4.2. Creación de Variables Derivadas - NHANES\n")

# HOMA-IR
nhanes_integrado['HOMA_IR'] = (nhanes_integrado['Glucosa'] * nhanes_integrado['Insulina']) / 405
# Ratios
nhanes_integrado['Ratio_TG_HDL'] = nhanes_integrado['Trigliceridos'] / nhanes_integrado['HDL']
nhanes_integrado['Ratio_Col_HDL'] = nhanes_integrado['Colesterol_Total'] / nhanes_integrado['HDL']

# Categorías derivadas
def categorizar_diabetes(hba1c):
    if pd.isna(hba1c): return np.nan
    elif hba1c < 5.7: return 'Normal'
    elif hba1c < 6.5: return 'Prediabetes'
    else: return 'Diabetes'

def categorizar_colesterol(ldl):
    if pd.isna(ldl): return np.nan
    elif ldl < 100: return 'Óptimo'
    elif ldl < 130: return 'Normal'
    elif ldl < 160: return 'Límite Alto'
    else: return 'Alto'

def categorizar_inflamacion(pcr):
    if pd.isna(pcr): return np.nan
    elif pcr < 1.0: return 'Bajo Riesgo'
    elif pcr < 3.0: return 'Riesgo Moderado'
    else: return 'Alto Riesgo'

nhanes_integrado['Categoria_Diabetes'] = nhanes_integrado['HbA1c'].apply(categorizar_diabetes)
nhanes_integrado['Categoria_Colesterol'] = nhanes_integrado['Colesterol_Total'].apply(categorizar_colesterol)
nhanes_integrado['Categoria_Inflamacion'] = nhanes_integrado['PCR'].apply(categorizar_inflamacion)

# Flags binarios
nhanes_integrado['Resistencia_Insulinica'] = (nhanes_integrado['HOMA_IR'] > 2.5).astype(float)
nhanes_integrado['Dislipidemia_Aterogenica'] = (nhanes_integrado['Ratio_TG_HDL'] > 3).astype(float)

# Guardar NHANES actualizado
nhanes_integrado.to_sql("NHANES_INTEGRADO", conn, if_exists="replace", index=False)
print("Variables derivadas creadas: HOMA_IR, Ratios, Categorías y Flags binarios\n")

# ======================================================
# 4.3. Preparar BRFSS para agregación
# ======================================================
print("4.3. Preparación BRFSS - Agregación por Características\n")

if brfss_clean is not None:
    brfss_agregado = brfss_clean.copy()
    vars_numericas = ['_SEX', '_AGEG5YR', '_BMI5', 'DIABETE4', '_RFBLDS6']
    for var in vars_numericas:
        if var in brfss_agregado.columns:
            brfss_agregado[var] = pd.to_numeric(brfss_agregado[var], errors='coerce')

    # Perfil de riesgo poblacional
    brfss_perfil = pd.DataFrame()
    if 'DIABETE4' in brfss_agregado.columns:
        brfss_perfil['Prevalencia_Diabetes'] = [(brfss_agregado['DIABETE4'] == 1.0).mean() * 100]
    if '_RFBLDS6' in brfss_agregado.columns:
        brfss_perfil['Prevalencia_Colesterol_Alto'] = [(brfss_agregado['_RFBLDS6'] == 1.0).mean() * 100]
    if '_BMI5' in brfss_agregado.columns:
        brfss_perfil['IMC_Promedio'] = [brfss_agregado['_BMI5'].mean() / 100]

    print("Perfil de riesgo poblacional BRFSS:")
    print(brfss_perfil.T)
else:
    brfss_agregado, brfss_perfil = None, None

# ======================================================
# 4.4. Procesar FoodData Central - Extraer nutrientes clave
# ======================================================
print("4.4. Procesamiento FoodData Central - Nutrientes Clave para Salud Metabólica\n")

fdc_nutrientes = None
if fdc_clean is not None and 'food_nutrient' in fdc_clean and 'food' in fdc_clean:
    nutrientes_clave = {
        1003: 'Proteina', 1004: 'Grasa_Total', 1005: 'Carbohidratos', 1079: 'Fibra',
        1087: 'Calcio', 1089: 'Hierro', 1095: 'Zinc', 1253: 'Colesterol_Dietetico',
        1258: 'Acidos_Grasos_Saturados', 1292: 'Acidos_Grasos_Monoinsaturados',
        1293: 'Acidos_Grasos_Poliinsaturados', 2000: 'Azucares_Totales', 1008: 'Energia'
    }

    fn = fdc_clean['food_nutrient']
    foods = fdc_clean['food']

    fn_filtrado = fn[fn['nutrient_id'].isin(nutrientes_clave.keys())].copy()
    nutrientes_pivot = fn_filtrado.pivot_table(
        index='fdc_id', columns='nutrient_id', values='amount', aggfunc='first'
    ).reset_index()
    nutrientes_pivot.columns = ['fdc_id'] + [nutrientes_clave.get(col, f'nutrient_{col}') for col in nutrientes_pivot.columns[1:]]

    fdc_nutrientes = foods[['fdc_id','description','food_category_id']].merge(
        nutrientes_pivot, on='fdc_id', how='inner'
    )

    # Índices nutricionales
    if 'Carbohidratos' in fdc_nutrientes.columns and 'Fibra' in fdc_nutrientes.columns:
        fdc_nutrientes['Carb_Netos'] = fdc_nutrientes['Carbohidratos'] - fdc_nutrientes['Fibra'].fillna(0)
        fdc_nutrientes['Indice_Glicemico_Est'] = fdc_nutrientes['Carb_Netos'] / (fdc_nutrientes['Fibra'].fillna(0.1) + 1)

    if all(c in fdc_nutrientes.columns for c in ['Acidos_Grasos_Monoinsaturados','Acidos_Grasos_Poliinsaturados','Acidos_Grasos_Saturados']):
        fdc_nutrientes['Grasas_Saludables'] = fdc_nutrientes['Acidos_Grasos_Monoinsaturados'].fillna(0) + fdc_nutrientes['Acidos_Grasos_Poliinsaturados'].fillna(0)
        fdc_nutrientes['Ratio_Grasas'] = fdc_nutrientes['Grasas_Saludables'] / (fdc_nutrientes['Acidos_Grasos_Saturados'].fillna(0.1) + 1)

    if 'Energia' in fdc_nutrientes.columns and 'Fibra' in fdc_nutrientes.columns:
        fdc_nutrientes['Densidad_Fibra'] = (fdc_nutrientes['Fibra'].fillna(0) / fdc_nutrientes['Energia'].replace(0,np.nan)) * 100

    print(f"Dataset FDC con nutrientes clave: {fdc_nutrientes.shape[0]:,} alimentos x {fdc_nutrientes.shape[1]} variables")
    print("Índices nutricionales calculados: Indice_Glicemico_Est, Ratio_Grasas, Densidad_Fibra\n")

# ======================================================
# 5.1. Crear tabla de mapeo FDC-ODEPA
# ======================================================
odepa_precios = pd.read_sql("SELECT * FROM ODEPA_Precios", conn)  # o "ODEPA_PRECIOS_CLEAN"

mapeo_records = []
if fdc_nutrientes is not None:
    for producto_odepa, terminos_fdc in mapeo_alimentos.items():
        odepa_match = odepa_precios[odepa_precios['Producto'].str.contains(producto_odepa, case=False, na=False)]
        if len(odepa_match) > 0:
            precio_promedio = odepa_match['Precio_Promedio_CLP'].iloc[0]
            producto_exacto = odepa_match['Producto'].iloc[0]
            grupo = odepa_match['Grupo'].iloc[0] if 'Grupo' in odepa_match.columns else np.nan

            for termino in terminos_fdc:
                fdc_match = fdc_nutrientes[fdc_nutrientes['description'].str.contains(termino, case=False, na=False)]
                if len(fdc_match) > 0:
                    fdc_row = fdc_match.iloc[0]
                    mapeo_records.append({
                        'Producto_ODEPA': producto_exacto,
                        'Grupo_ODEPA': grupo,
                        'Precio_CLP_kg': precio_promedio,
                        'fdc_id': fdc_row['fdc_id'],
                        'Descripcion_FDC': fdc_row['description'],
                        'food_category_id': fdc_row['food_category_id'],
                        'Proteina': fdc_row.get('Proteina', np.nan),
                        'Grasa_Total': fdc_row.get('Grasa_Total', np.nan),
                        'Carbohidratos': fdc_row.get('Carbohidratos', np.nan),
                        'Fibra': fdc_row.get('Fibra', np.nan),
                        'Energia': fdc_row.get('Energia', np.nan)
                    })
                    break

alimentos_chile = pd.DataFrame(mapeo_records)

if not alimentos_chile.empty:
    alimentos_chile['Precio_CLP_100g'] = alimentos_chile['Precio_CLP_kg'] / 10
    alimentos_chile['Costo_por_g_Proteina'] = alimentos_chile['Precio_CLP_100g'] / alimentos_chile['Proteina'].replace(0, np.nan)
    alimentos_chile['Costo_por_g_Fibra'] = alimentos_chile['Precio_CLP_100g'] / alimentos_chile['Fibra'].replace(0, np.nan)
    alimentos_chile['Costo_por_100kcal'] = (alimentos_chile['Precio_CLP_100g'] / alimentos_chile['Energia'].replace(0, np.nan)) * 100
else:
    print("alimentos_chile está vacío. Revisa mapeo_alimentos y odepa_precios")


# Calcular métricas de costo-beneficio
alimentos_chile['Precio_CLP_100g'] = alimentos_chile['Precio_CLP_kg'] / 10
alimentos_chile['Costo_por_g_Proteina'] = alimentos_chile['Precio_CLP_100g'] / alimentos_chile['Proteina'].replace(0, np.nan)
alimentos_chile['Costo_por_g_Fibra'] = alimentos_chile['Precio_CLP_100g'] / alimentos_chile['Fibra'].replace(0, np.nan)
alimentos_chile['Costo_por_100kcal'] = (alimentos_chile['Precio_CLP_100g'] / alimentos_chile['Energia'].replace(0, np.nan)) * 100

# ======================================================
# 5.2. Guardar en SQLite (en memoria)
# ======================================================
conn_integrado = sqlite3.connect("pipeline.db")

tablas_sqlite = {
    'nhanes_metabolico': nhanes_integrado,
    'alimentos_chile': alimentos_chile,
    'odepa_precios': odepa_precios
}

for nombre, df in tablas_sqlite.items():
    df.to_sql(nombre, conn_integrado, index=False, if_exists='replace')
