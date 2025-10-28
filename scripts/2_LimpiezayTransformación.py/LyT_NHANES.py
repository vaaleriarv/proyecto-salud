import pandas as pd
import sqlite3
import numpy as np

# ========================================
# CONFIGURACIÓN INICIAL
# ========================================
conn = sqlite3.connect("pipeline.db")
print("="*70)
print("LIMPIEZA Y TRANSFORMACIÓN NHANES - ENFOQUE DIABETES/COLESTEROL")
print("="*70)

# ========================================
# 1. LIMPIEZA GLOBAL NHANES
# ========================================
print("\n" + "="*70)
print("PASO 1: LIMPIEZA GLOBAL DE TABLAS NHANES")
print("="*70)

# Parámetros de limpieza
threshold_nan_col = 0.70
threshold_nan_row = 0.70

# Tablas NHANES
tablas = [
    "DEMO_L", "BMX_L", "GLU_L", "HDL_L", "TRIGLY_L", "TCHOL_L",
    "DR1TOT_L", "DR2TOT_L", "ALB_CR_L", "AGP_L", "CBC_L", "FASTQX_L",
    "FERTIN_L", "FOLATE_L", "GHB_L", "HEPA_L", "HEPB_S_L", "HSCRP_L",
    "INS_L", "PBCD_L", "IHGEM_L", "FOLFMS_L", "TST_L", "BIOPRO_L",
    "TFR_L", "UCPREG_L", "VID_L", "VOCWB_L"
]

nhanes_clean = {}
nhanes_cleaning_summary = []

for name in tablas:
    try:
        df = pd.read_sql(f"SELECT * FROM {name}", conn)
        original_shape = df.shape

        if name == 'FASTQX_L':
            # Regla especial FASTQX (90% de threshold)
            col_nan_pct = df.isnull().sum() / len(df)
            cols_to_drop = col_nan_pct[col_nan_pct > 0.90].index.tolist()
            df_limpio = df.drop(columns=cols_to_drop)
            row_nan_pct = df_limpio.isnull().sum(axis=1) / df_limpio.shape[1]
            df_limpio = df_limpio[row_nan_pct < 1.0]
        else:
            # Limpieza estándar
            col_nan_pct = df.isnull().sum() / len(df)
            cols_to_drop = col_nan_pct[col_nan_pct > threshold_nan_col].index.tolist()
            df_limpio = df.drop(columns=cols_to_drop)
            row_nan_pct = df_limpio.isnull().sum(axis=1) / df_limpio.shape[1]
            df_limpio = df_limpio[row_nan_pct <= threshold_nan_row]

        final_shape = df_limpio.shape
        completitud = (1 - df_limpio.isnull().sum().sum() / (df_limpio.shape[0] * df_limpio.shape[1])) * 100

        nhanes_clean[name] = df_limpio
        
        nhanes_cleaning_summary.append({
            'dataset': name,
            'filas_original': original_shape[0],
            'filas_final': final_shape[0],
            'cols_original': original_shape[1],
            'cols_final': final_shape[1],
            'cols_eliminadas': len(cols_to_drop),
            'completitud_pct': completitud
        })
        
        print(f"✓ {name}: {original_shape[0]:,} → {final_shape[0]:,} filas | "
              f"{original_shape[1]} → {final_shape[1]} cols | "
              f"Completitud: {completitud:.1f}%")
        
    except Exception as e:
        print(f"✗ Error en {name}: {e}")

nhanes_summary_df = pd.DataFrame(nhanes_cleaning_summary)
print(f"\n{'='*70}")
print("RESUMEN DE LIMPIEZA GLOBAL:")
print(nhanes_summary_df[['dataset', 'filas_original', 'filas_final', 'cols_original', 'cols_final', 'completitud_pct']])

# ========================================
# 2. LIMPIEZA ESPECÍFICA - VARIABLES CLAVE
# ========================================
print(f"\n{'='*70}")
print("PASO 2: LIMPIEZA ESPECÍFICA DE VARIABLES CLAVE")
print("="*70)

# ----------------------------------------
# 2.1 DEMO_L - Variables demográficas
# ----------------------------------------
print("\n2.1. Limpiando DEMO_L (Demographics)...")
try:
    df_demo = nhanes_clean['DEMO_L'].copy()
    
    # Mapeos de variables categóricas
    demo_mappings = {
        "RIAGENDR": {
            1: "Hombre",
            2: "Mujer"
        },
        "DMDBORN4": {
            1: "Nacido en EE.UU.",
            2: "Otros países",
            77: np.nan,  # Se negó a responder
            99: np.nan   # No sabe
        },
        "DMDEDUC2": {
            1: "Menos de 9º grado",
            2: "9-11º grado",
            3: "Graduado secundaria",
            4: "Alguna universidad/AA",
            5: "Graduado universitario",
            7: np.nan,
            9: np.nan
        },
        "DMDHHSIZ": {
            1: "1 persona", 2: "2 personas", 3: "3 personas",
            4: "4 personas", 5: "5 personas", 6: "6 personas",
            7: "7 o más personas"
        },
        "DMDHRAGZ": {
            1: "<20 años",
            2: "20-39 años",
            3: "40-59 años",
            4: "60+ años"
        },
        "DMDHRGND": {
            1: "Hombre",
            2: "Mujer"
        },
        "DMDMARTZ": {
            1: "Casado/Vive con pareja",
            2: "Viudo/Divorciado/Separado",
            3: "Nunca se casó",
            77: np.nan,
            99: np.nan
        },
        "RIDRETH3": {  # Raza/Etnicidad (importante para diabetes)
            1: "Mexicano-Americano",
            2: "Otro Hispano",
            3: "Blanco no hispano",
            4: "Negro no hispano",
            6: "Asiático no hispano",
            7: "Otra raza"
        }
    }
    
    for col, mapping in demo_mappings.items():
        if col in df_demo.columns:
            df_demo[col] = df_demo[col].map(mapping)
    
    # Variables numéricas
    if 'RIDAGEYR' in df_demo.columns:
        df_demo['RIDAGEYR'] = pd.to_numeric(df_demo['RIDAGEYR'], errors='coerce')
    
    if 'INDFMPIR' in df_demo.columns:  # Ratio ingreso/pobreza
        df_demo['INDFMPIR'] = pd.to_numeric(df_demo['INDFMPIR'], errors='coerce')
    
    nhanes_clean['DEMO_L'] = df_demo
    print("  ✓ DEMO_L: Variables demográficas mapeadas")
    
except Exception as e:
    print(f"  ✗ Error en DEMO_L: {e}")

# ----------------------------------------
# 2.2 BMX_L - Antropometría
# ----------------------------------------
print("\n2.2. Limpiando BMX_L (Body Measures)...")
try:
    df_bmx = nhanes_clean['BMX_L'].copy()
    
    # Variables antropométricas clave
    vars_antropo = {
        'BMXWT': 'Peso (kg)',
        'BMXHT': 'Altura (cm)',
        'BMXBMI': 'IMC',
        'BMXWAIST': 'Circunferencia cintura (cm)',
        'BMXHIP': 'Circunferencia cadera (cm)'
    }
    
    for var, desc in vars_antropo.items():
        if var in df_bmx.columns:
            df_bmx[var] = pd.to_numeric(df_bmx[var], errors='coerce')
            
            # Validación de outliers
            if var == 'BMXWT':  # Peso
                df_bmx.loc[(df_bmx[var] < 20) | (df_bmx[var] > 300), var] = np.nan
            elif var == 'BMXHT':  # Altura
                df_bmx.loc[(df_bmx[var] < 100) | (df_bmx[var] > 230), var] = np.nan
            elif var == 'BMXBMI':  # IMC
                df_bmx.loc[(df_bmx[var] < 12) | (df_bmx[var] > 70), var] = np.nan
            elif var == 'BMXWAIST':  # Cintura
                df_bmx.loc[(df_bmx[var] < 40) | (df_bmx[var] > 200), var] = np.nan
    
    nhanes_clean['BMX_L'] = df_bmx
    print("  ✓ BMX_L: Variables antropométricas validadas")
    
except Exception as e:
    print(f"  ✗ Error en BMX_L: {e}")

# ----------------------------------------
# 2.3 GHB_L - Hemoglobina Glucosilada (HbA1c) ***CRÍTICA***
# ----------------------------------------
print("\n2.3. Limpiando GHB_L (Glycohemoglobin - HbA1c)...")
try:
    df_ghb = nhanes_clean['GHB_L'].copy()
    
    if 'LBXGH' in df_ghb.columns:
        df_ghb['LBXGH'] = pd.to_numeric(df_ghb['LBXGH'], errors='coerce')
        # Validar rango: HbA1c normal = 4-6%, diabetes ≥6.5%
        df_ghb.loc[(df_ghb['LBXGH'] < 3) | (df_ghb['LBXGH'] > 18), 'LBXGH'] = np.nan
        
        nhanes_clean['GHB_L'] = df_ghb
        print(f"   GHB_L: HbA1c limpiada (n={df_ghb['LBXGH'].notna().sum():,})")
    else:
        print("   LBXGH no encontrada en GHB_L")
        
except Exception as e:
    print(f"  ✗ Error en GHB_L: {e}")

# ----------------------------------------
# 2.4 GLU_L - Glucosa en sangre
# ----------------------------------------
print("\n2.4. Limpiando GLU_L (Glucose)...")
try:
    df_glu = nhanes_clean['GLU_L'].copy()
    
    if 'LBXGLU' in df_glu.columns:
        df_glu['LBXGLU'] = pd.to_numeric(df_glu['LBXGLU'], errors='coerce')
        # Validar rango razonable: 40-600 mg/dL
        df_glu.loc[(df_glu['LBXGLU'] < 40) | (df_glu['LBXGLU'] > 600), 'LBXGLU'] = np.nan
        
        nhanes_clean['GLU_L'] = df_glu
        print(f"   GLU_L: Glucosa limpiada (n={df_glu['LBXGLU'].notna().sum():,})")
    else:
        print("   LBXGLU no encontrada en GLU_L")
        
except Exception as e:
    print(f"   Error en GLU_L: {e}")

# ----------------------------------------
# 2.5 TCHOL_L - Colesterol Total
# ----------------------------------------
print("\n2.5. Limpiando TCHOL_L (Total Cholesterol)...")
try:
    df_tchol = nhanes_clean['TCHOL_L'].copy()
    
    if 'LBXTC' in df_tchol.columns:
        df_tchol['LBXTC'] = pd.to_numeric(df_tchol['LBXTC'], errors='coerce')
        # Validar rango: 100-500 mg/dL
        df_tchol.loc[(df_tchol['LBXTC'] < 100) | (df_tchol['LBXTC'] > 500), 'LBXTC'] = np.nan
        
        nhanes_clean['TCHOL_L'] = df_tchol
        print(f"   TCHOL_L: Colesterol total limpiado (n={df_tchol['LBXTC'].notna().sum():,})")
    else:
        print("   LBXTC no encontrada en TCHOL_L")
        
except Exception as e:
    print(f"   Error en TCHOL_L: {e}")

# ----------------------------------------
# 2.6 HDL_L - Colesterol HDL (Bueno)
# ----------------------------------------
print("\n2.6. Limpiando HDL_L (HDL Cholesterol)...")
try:
    df_hdl = nhanes_clean['HDL_L'].copy()
    
    if 'LBDHDD' in df_hdl.columns:
        df_hdl['LBDHDD'] = pd.to_numeric(df_hdl['LBDHDD'], errors='coerce')
        # Validar rango: 10-150 mg/dL
        df_hdl.loc[(df_hdl['LBDHDD'] < 10) | (df_hdl['LBDHDD'] > 150), 'LBDHDD'] = np.nan
        
        nhanes_clean['HDL_L'] = df_hdl
        print(f"  HDL_L: HDL limpiado (n={df_hdl['LBDHDD'].notna().sum():,})")
    else:
        print("  LBDHDD no encontrada en HDL_L")
    
except Exception as e:
    print(f"  Error en HDL_L: {e}")

# ----------------------------------------
# 2.7 TRIGLY_L - Triglicéridos
# ----------------------------------------
print("\n2.7. Limpiando TRIGLY_L (Triglycerides)...")
try:
    df_trigly = nhanes_clean['TRIGLY_L'].copy()
    
    if 'LBXTR' in df_trigly.columns:
        df_trigly['LBXTR'] = pd.to_numeric(df_trigly['LBXTR'], errors='coerce')
        # Validar rango: 30-1000 mg/dL
        df_trigly.loc[(df_trigly['LBXTR'] < 30) | (df_trigly['LBXTR'] > 1000), 'LBXTR'] = np.nan
        
        nhanes_clean['TRIGLY_L'] = df_trigly
        print(f"  ✓ TRIGLY_L: Triglicéridos limpiados (n={df_trigly['LBXTR'].notna().sum():,})")
    elif 'LBDTRSI' in df_trigly.columns:  # Nombre alternativo
        df_trigly['LBDTRSI'] = pd.to_numeric(df_trigly['LBDTRSI'], errors='coerce')
        df_trigly.loc[(df_trigly['LBDTRSI'] < 30) | (df_trigly['LBDTRSI'] > 1000), 'LBDTRSI'] = np.nan
        
        nhanes_clean['TRIGLY_L'] = df_trigly
        print(f"   TRIGLY_L: Triglicéridos limpiados (n={df_trigly['LBDTRSI'].notna().sum():,})")
    else:
        print("   LBXTR/LBDTRSI no encontrada en TRIGLY_L")
        
except Exception as e:
    print(f"  Error en TRIGLY_L: {e}")

# ----------------------------------------
# 2.8 DR1TOT_L y DR2TOT_L - Dieta
# ----------------------------------------
print("\n2.8. Limpiando DR1TOT_L y DR2TOT_L (Dietary Intake)...")
for tbl_name, dia in [("DR1TOT_L", "día 1"), ("DR2TOT_L", "día 2")]:
    try:
        df_diet = nhanes_clean[tbl_name].copy()
        
        # Variables dietéticas clave para diabetes/colesterol
        vars_dieta = {
            f'DR{dia[4]}TKCAL': 'Calorías totales',
            f'DR{dia[4]}TCARB': 'Carbohidratos (g)',
            f'DR{dia[4]}TSUGR': 'Azúcares totales (g)',
            f'DR{dia[4]}TFIBE': 'Fibra (g)',
            f'DR{dia[4]}TTFAT': 'Grasa total (g)',
            f'DR{dia[4]}TSFAT': 'Grasa saturada (g)',
            f'DR{dia[4]}TCHOL': 'Colesterol dietético (mg)',
            f'DR{dia[4]}TSODI': 'Sodio (mg)',
            f'DR{dia[4]}TPROT': 'Proteína (g)'
        }
        
        for var, desc in vars_dieta.items():
            if var in df_diet.columns:
                df_diet[var] = pd.to_numeric(df_diet[var], errors='coerce')
        
        nhanes_clean[tbl_name] = df_diet
        print(f"   {tbl_name}: Variables dietéticas limpiadas ({dia})")
        
    except Exception as e:
        print(f"   Error en {tbl_name}: {e}")

# ----------------------------------------
# 2.9 Otras tablas de laboratorio
# ----------------------------------------
print("\n2.9. Limpiando otras tablas de laboratorio...")
otras_tablas_lab = [
    "ALB_CR_L", "AGP_L", "CBC_L", "FASTQX_L", "FERTIN_L", "FOLATE_L",
    "HEPA_L", "HEPB_S_L", "HSCRP_L", "INS_L", "PBCD_L", "IHGEM_L",
    "FOLFMS_L", "TST_L", "BIOPRO_L", "TFR_L", "UCPREG_L", "VID_L", "VOCWB_L"
]

for tbl in otras_tablas_lab:
    try:
        if tbl in nhanes_clean:
            df = nhanes_clean[tbl].copy()
            
            # Convertir todas las columnas numéricas
            for col in df.columns:
                if col != 'SEQN':  # No convertir el ID
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            nhanes_clean[tbl] = df
            print(f"  ✓ {tbl}: Convertido a numérico")
    except Exception as e:
        print(f"  ✗ Error en {tbl}: {e}")

# ========================================
# 3. UNIR TODAS LAS TABLAS NHANES (JOIN)
# ========================================
print(f"\n{'='*70}")
print("PASO 3: UNIÓN DE TABLAS NHANES POR SEQN")
print("="*70)

try:
    # Comenzar con DEMO_L como base
    nhanes_master = nhanes_clean['DEMO_L'].copy()
    print(f"Base inicial (DEMO_L): {nhanes_master.shape[0]:,} participantes")
    
    # JOINs secuenciales
    tablas_merge = [
        ('BMX_L', 'Antropometría'),
        ('GHB_L', 'HbA1c'),
        ('GLU_L', 'Glucosa'),
        ('TCHOL_L', 'Colesterol Total'),
        ('HDL_L', 'HDL'),
        ('TRIGLY_L', 'Triglicéridos'),
        ('DR1TOT_L', 'Dieta Día 1'),
        ('DR2TOT_L', 'Dieta Día 2')
    ]
    
    for tabla, descripcion in tablas_merge:
        if tabla in nhanes_clean:
            antes = nhanes_master.shape[1]
            nhanes_master = nhanes_master.merge(
                nhanes_clean[tabla],
                on='SEQN',
                how='left',
                suffixes=('', f'_{tabla}')
            )
            despues = nhanes_master.shape[1]
            print(f"  ✓ {tabla} ({descripcion}): +{despues - antes} columnas")
        else:
            print(f"  ⚠ {tabla} no disponible, se omite")
    
    print(f"\nTabla maestra creada: {nhanes_master.shape[0]:,} filas × {nhanes_master.shape[1]} columnas")
    
except Exception as e:
    print(f"✗ Error en unión de tablas: {e}")
    nhanes_master = nhanes_clean['DEMO_L'].copy()

# ========================================
# 4. CREAR VARIABLES DERIVADAS
# ========================================
print(f"\n{'='*70}")
print("PASO 4: CREACIÓN DE VARIABLES DERIVADAS")
print("="*70)

# ----------------------------------------
# 4.1 Variables demográficas categóricas
# ----------------------------------------
print("\n4.1. Variables demográficas...")

# Grupos de edad
if 'RIDAGEYR' in nhanes_master.columns:
    bins = [0, 18, 25, 35, 45, 55, 65, 150]
    labels = ['0-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']
    nhanes_master['grupo_edad'] = pd.cut(
        nhanes_master['RIDAGEYR'],
        bins=bins,
        labels=labels,
        right=False
    )
    print("  ✓ grupo_edad creado")

# Categoría de ingresos
if 'INDFMPIR' in nhanes_master.columns:
    def clasificar_ingreso(pir):
        if pd.isna(pir):
            return np.nan
        elif pir < 1.0:
            return 'Bajo pobreza'
        elif pir < 2.0:
            return 'Cerca pobreza'
        elif pir < 4.0:
            return 'Medio'
        else:
            return 'Alto'
    
    nhanes_master['categoria_ingreso'] = nhanes_master['INDFMPIR'].apply(clasificar_ingreso)
    print("  ✓ categoria_ingreso creada")

# ----------------------------------------
# 4.2 IMC y categorías
# ----------------------------------------
print("\n4.2. Variables antropométricas...")

# Calcular IMC si no existe
if 'BMXBMI' not in nhanes_master.columns and 'BMXWT' in nhanes_master.columns and 'BMXHT' in nhanes_master.columns:
    nhanes_master['BMXBMI'] = nhanes_master['BMXWT'] / ((nhanes_master['BMXHT'] / 100) ** 2)
    print(" BMXBMI calculado")

# Categoría de IMC
if 'BMXBMI' in nhanes_master.columns:
    def clasificar_imc(imc):
        if pd.isna(imc):
            return np.nan
        elif imc < 18.5:
            return 'Bajo peso'
        elif imc < 25:
            return 'Normal'
        elif imc < 30:
            return 'Sobrepeso'
        else:
            return 'Obesidad'
    
    nhanes_master['categoria_imc'] = nhanes_master['BMXBMI'].apply(clasificar_imc)
    print(" categoria_imc creada")

# Ratio cintura/cadera (importante para riesgo metabólico)
if 'BMXWAIST' in nhanes_master.columns and 'BMXHIP' in nhanes_master.columns:
    nhanes_master['ratio_cintura_cadera'] = nhanes_master['BMXWAIST'] / nhanes_master['BMXHIP']
    
    def clasificar_rcc(row):
        rcc = row.get('ratio_cintura_cadera')
        sexo = row.get('RIAGENDR')
        
        if pd.isna(rcc) or pd.isna(sexo):
            return np.nan
        
        # Criterios de riesgo por sexo
        if sexo == 'Hombre':
            return 'Alto riesgo' if rcc > 0.90 else 'Normal'
        elif sexo == 'Mujer':
            return 'Alto riesgo' if rcc > 0.85 else 'Normal'
        else:
            return np.nan
    
    nhanes_master['riesgo_rcc'] = nhanes_master.apply(clasificar_rcc, axis=1)
    print(" ratio_cintura_cadera y riesgo_rcc creados")

# ----------------------------------------
# 4.3 DIABETES 
# ----------------------------------------
print("\n4.3. Variables de diabetes...")

# Diagnóstico de diabetes por HbA1c
if 'LBXGH' in nhanes_master.columns:
    nhanes_master['diabetes_hba1c'] = (nhanes_master['LBXGH'] >= 6.5).astype(float)
    nhanes_master.loc[nhanes_master['LBXGH'].isna(), 'diabetes_hba1c'] = np.nan
    
    # Categorías de HbA1c
    def clasificar_hba1c(valor):
        if pd.isna(valor):
            return np.nan
        elif valor < 5.7:
            return 'Normal'
        elif valor < 6.5:
            return 'Prediabetes'
        else:
            return 'Diabetes'
    
    nhanes_master['categoria_hba1c'] = nhanes_master['LBXGH'].apply(clasificar_hba1c)
    
    count_diabetes = nhanes_master['diabetes_hba1c'].sum()
    total = nhanes_master['diabetes_hba1c'].notna().sum()
    print(f"diabetes_hba1c creada: {count_diabetes:.0f} casos de {total:,} ({count_diabetes/total*100:.1f}%)")

# Diagnóstico de diabetes por glucosa en ayunas
if 'LBXGLU' in nhanes_master.columns:
    nhanes_master['diabetes_glucosa'] = (nhanes_master['LBXGLU'] >= 126).astype(float)
    nhanes_master.loc[nhanes_master['LBXGLU'].isna(), 'diabetes_glucosa'] = np.nan
    
    # Categorías de glucosa
    def clasificar_glucosa(valor):
        if pd.isna(valor):
            return np.nan
        elif valor < 100:
            return 'Normal'
        elif valor < 126:
            return 'Prediabetes'
        else:
            return 'Diabetes'
    
    nhanes_master['categoria_glucosa'] = nhanes_master['LBXGLU'].apply(clasificar_glucosa)
    print(" diabetes_glucosa y categoria_glucosa creadas")

# DIABETES COMBINADA
if 'diabetes_hba1c' in nhanes_master.columns or 'diabetes_glucosa' in nhanes_master.columns:
    nhanes_master['tiene_diabetes'] = 0
    
    if 'diabetes_hba1c' in nhanes_master.columns:
        nhanes_master.loc[nhanes_master['diabetes_hba1c'] == 1, 'tiene_diabetes'] = 1
    
    if 'diabetes_glucosa' in nhanes_master.columns:
        nhanes_master.loc[nhanes_master['diabetes_glucosa'] == 1, 'tiene_diabetes'] = 1
    
    mask_sin_datos = True
    if 'diabetes_hba1c' in nhanes_master.columns:
        mask_sin_datos = mask_sin_datos & nhanes_master['diabetes_hba1c'].isna()
    if 'diabetes_glucosa' in nhanes_master.columns:
        mask_sin_datos = mask_sin_datos & nhanes_master['diabetes_glucosa'].isna()
    
    nhanes_master.loc[mask_sin_datos, 'tiene_diabetes'] = np.nan
    
    count_diabetes = nhanes_master['tiene_diabetes'].sum()
    total = nhanes_master['tiene_diabetes'].notna().sum()
    print(f"  ✓ tiene_diabetes (combinada): {count_diabetes:.0f} casos de {total:,} ({count_diabetes/total*100:.1f}%)")

# ----------------------------------------
# 4.4 COLESTEROL - Variables críticas
# ----------------------------------------
print("\n4.4. Variables de colesterol...")

# Colesterol total alto
if 'LBXTC' in nhanes_master.columns:
    nhanes_master['colesterol_alto'] = (nhanes_master['LBXTC'] >= 240).astype(float)
    nhanes_master.loc[nhanes_master['LBXTC'].isna(), 'colesterol_alto'] = np.nan
    
    # Categorías de colesterol total
    def clasificar_colesterol_total(valor):
        if pd.isna(valor):
            return np.nan
        elif valor < 200:
            return 'Deseable'
        elif valor < 240:
            return 'Límite alto'
        else:
            return 'Alto'
    
    nhanes_master['categoria_colesterol'] = nhanes_master['LBXTC'].apply(clasificar_colesterol_total)
    
    count_col_alto = nhanes_master['colesterol_alto'].sum()
    total = nhanes_master['colesterol_alto'].notna().sum()
    print(f"  ✓ colesterol_alto: {count_col_alto:.0f} casos de {total:,} ({count_col_alto/total*100:.1f}%)")

# HDL bajo (riesgo cardiovascular)
if 'LBDHDD' in nhanes_master.columns:
    def clasificar_hdl(row):
        hdl = row.get('LBDHDD')
        sexo = row.get('RIAGENDR')
        
        if pd.isna(hdl):
            return np.nan
        
        # Criterios por sexo
        if sexo == 'Hombre':
            if hdl < 40:
                return 'Bajo (riesgo)'
            elif hdl < 60:
                return 'Normal'
            else:
                return 'Alto (protector)'
        elif sexo == 'Mujer':
            if hdl < 50:
                return 'Bajo (riesgo)'
            elif hdl < 60:
                return 'Normal'
            else:
                return 'Alto (protector)'
        else:
            return np.nan
    
    nhanes_master['categoria_hdl'] = nhanes_master.apply(clasificar_hdl, axis=1)
    
    # HDL bajo binario
    nhanes_master['hdl_bajo'] = 0
    nhanes_master.loc[
        ((nhanes_master['RIAGENDR'] == 'Hombre') & (nhanes_master['LBDHDD'] < 40)) |
        ((nhanes_master['RIAGENDR'] == 'Mujer') & (nhanes_master['LBDHDD'] < 50)),
        'hdl_bajo'
    ] = 1
    nhanes_master.loc[nhanes_master['LBDHDD'].isna(), 'hdl_bajo'] = np.nan
    
    print("  ✓ categoria_hdl y hdl_bajo creadas")

# Triglicéridos altos
if 'LBXTR' in nhanes_master.columns:
    var_trig = 'LBXTR'
elif 'LBDTRSI' in nhanes_master.columns:
    var_trig = 'LBDTRSI'
else:
    var_trig = None

if var_trig:
    nhanes_master['trigliceridos_altos'] = (nhanes_master[var_trig] >= 150).astype(float)
    nhanes_master.loc[nhanes_master[var_trig].isna(), 'trigliceridos_altos'] = np.nan
    
    def clasificar_trigliceridos(valor):
        if pd.isna(valor):
            return np.nan
        elif valor < 150:
            return 'Normal'
        elif valor < 200:
            return 'Límite alto'
        elif valor < 500:
            return 'Alto'
        else:
            return 'Muy alto'
    
    nhanes_master['categoria_trigliceridos'] = nhanes_master[var_trig].apply(clasificar_trigliceridos)
    print("trigliceridos_altos y categoria_trigliceridos creadas")

# LDL calculado
if 'LBXTC' in nhanes_master.columns and 'LBDHDD' in nhanes_master.columns and var_trig:
    # LDL = Colesterol Total - HDL - (Triglicéridos / 5)
    # Solo válido si triglicéridos < 400 mg/dL
    nhanes_master['LBDLDL_calc'] = np.nan
    mask_valido = nhanes_master[var_trig] < 400
    
    nhanes_master.loc[mask_valido, 'LBDLDL_calc'] = (
        nhanes_master.loc[mask_valido, 'LBXTC'] -
        nhanes_master.loc[mask_valido, 'LBDHDD'] -
        (nhanes_master.loc[mask_valido, var_trig] / 5)
    )
    
    # Categoría LDL
    def clasificar_ldl(valor):
        if pd.isna(valor):
            return np.nan
        elif valor < 100:
            return 'Óptimo'
        elif valor < 130:
            return 'Cerca del óptimo'
        elif valor < 160:
            return 'Límite alto'
        elif valor < 190:
            return 'Alto'
        else:
            return 'Muy alto'
    
    nhanes_master['categoria_ldl'] = nhanes_master['LBDLDL_calc'].apply(clasificar_ldl)
    
    count_ldl = nhanes_master['LBDLDL_calc'].notna().sum()
    print(f"LBDLDL_calc (LDL calculado): {count_ldl:,} valores")

# ----------------------------------------
# 4.5 SÍNDROME METABÓLICO (Criterios ATP III)
# ----------------------------------------
print("\n4.5. Síndrome metabólico...")

# Se diagnostica con 3 o más de estos criterios:
# 1. Circunferencia cintura: >102 cm (H), >88 cm (M)
# 2. Triglicéridos ≥150 mg/dL
# 3. HDL <40 mg/dL (H), <50 mg/dL (M)
# 4. Presión arterial ≥130/85 (no tenemos datos de presión)
# 5. Glucosa en ayunas ≥100 mg/dL

nhanes_master['sm_criterio_cintura'] = 0
if 'BMXWAIST' in nhanes_master.columns:
    nhanes_master.loc[
        ((nhanes_master['RIAGENDR'] == 'Hombre') & (nhanes_master['BMXWAIST'] > 102)) |
        ((nhanes_master['RIAGENDR'] == 'Mujer') & (nhanes_master['BMXWAIST'] > 88)),
        'sm_criterio_cintura'
    ] = 1

nhanes_master['sm_criterio_trigliceridos'] = 0
if var_trig:
    nhanes_master.loc[nhanes_master[var_trig] >= 150, 'sm_criterio_trigliceridos'] = 1

nhanes_master['sm_criterio_hdl'] = 0
if 'LBDHDD' in nhanes_master.columns:
    nhanes_master.loc[
        ((nhanes_master['RIAGENDR'] == 'Hombre') & (nhanes_master['LBDHDD'] < 40)) |
        ((nhanes_master['RIAGENDR'] == 'Mujer') & (nhanes_master['LBDHDD'] < 50)),
        'sm_criterio_hdl'
    ] = 1

nhanes_master['sm_criterio_glucosa'] = 0
if 'LBXGLU' in nhanes_master.columns:
    nhanes_master.loc[nhanes_master['LBXGLU'] >= 100, 'sm_criterio_glucosa'] = 1

# Score de síndrome metabólico (0-4, sin presión arterial)
nhanes_master['sm_score'] = (
    nhanes_master['sm_criterio_cintura'] +
    nhanes_master['sm_criterio_trigliceridos'] +
    nhanes_master['sm_criterio_hdl'] +
    nhanes_master['sm_criterio_glucosa']
)

# Síndrome metabólico probable (≥3 criterios de 4 disponibles)
nhanes_master['sindrome_metabolico'] = (nhanes_master['sm_score'] >= 3).astype(int)

count_sm = nhanes_master['sindrome_metabolico'].sum()
total_sm = len(nhanes_master)
print(f"  ✓ sindrome_metabolico: {count_sm:,} casos de {total_sm:,} ({count_sm/total_sm*100:.1f}%)")

# ----------------------------------------
# 4.6 RIESGO CARDIOVASCULAR COMBINADO
# ----------------------------------------
print("\n4.6. Riesgo cardiovascular combinado...")

def calcular_riesgo_cv(row):
    """
    Score de riesgo cardiovascular (0-10)
    """
    score = 0
    
    # Edad (0-2 puntos)
    edad = row.get('RIDAGEYR')
    if pd.notna(edad):
        if edad >= 65:
            score += 2
        elif edad >= 45:
            score += 1
    
    # Diabetes (3 puntos)
    if row.get('tiene_diabetes') == 1:
        score += 3
    
    # Colesterol alto (2 puntos)
    if row.get('colesterol_alto') == 1:
        score += 2
    
    # HDL bajo (1 punto)
    if row.get('hdl_bajo') == 1:
        score += 1
    
    # Obesidad (1 punto)
    if row.get('categoria_imc') == 'Obesidad':
        score += 1
    
    # Síndrome metabólico (1 punto adicional)
    if row.get('sindrome_metabolico') == 1:
        score += 1
    
    return score

nhanes_master['riesgo_cardiovascular'] = nhanes_master.apply(calcular_riesgo_cv, axis=1)

# Categoría de riesgo CV
def clasificar_riesgo_cv(score):
    if pd.isna(score):
        return np.nan
    elif score <= 2:
        return 'Bajo'
    elif score <= 5:
        return 'Moderado'
    elif score <= 7:
        return 'Alto'
    else:
        return 'Muy alto'

nhanes_master['categoria_riesgo_cv'] = nhanes_master['riesgo_cardiovascular'].apply(clasificar_riesgo_cv)

print("  ✓ riesgo_cardiovascular y categoria_riesgo_cv creadas")

# ----------------------------------------
# 4.7 VARIABLES DIETÉTICAS
# ----------------------------------------
print("\n4.7. Variables dietéticas...")

# Promedio de dieta entre día 1 y día 2
vars_dieta = ['TKCAL', 'TCARB', 'TSUGR', 'TFIBE', 'TTFAT', 'TSFAT', 'TCHOL', 'TSODI', 'TPROT']

for var in vars_dieta:
    dr1_col = f'DR1{var}'
    dr2_col = f'DR2{var}'
    
    if dr1_col in nhanes_master.columns and dr2_col in nhanes_master.columns:
        nhanes_master[f'promedio_{var.lower()}'] = nhanes_master[[dr1_col, dr2_col]].mean(axis=1)
    elif dr1_col in nhanes_master.columns:
        nhanes_master[f'promedio_{var.lower()}'] = nhanes_master[dr1_col]

# Categorías de calorías
if 'promedio_tkcal' in nhanes_master.columns:
    bins = [0, 1200, 1500, 2000, 2500, 3000, 10000]
    labels = ['<1200', '1200-1499', '1500-1999', '2000-2499', '2500-2999', '3000+']
    nhanes_master['categoria_calorias'] = pd.cut(
        nhanes_master['promedio_tkcal'],
        bins=bins,
        labels=labels,
        right=False
    )
    print("  ✓ promedio_tkcal y categoria_calorias creadas")

# Ratio azúcares/calorías (% de calorías de azúcar)
if 'promedio_tsugr' in nhanes_master.columns and 'promedio_tkcal' in nhanes_master.columns:
    # 1g azúcar = 4 calorías
    nhanes_master['pct_calorias_azucar'] = (
        (nhanes_master['promedio_tsugr'] * 4) / nhanes_master['promedio_tkcal']
    ) * 100
    
    def clasificar_azucar(pct):
        if pd.isna(pct):
            return np.nan
        elif pct < 10:
            return 'Bajo'
        elif pct < 20:
            return 'Moderado'
        else:
            return 'Alto'
    
    nhanes_master['categoria_azucar'] = nhanes_master['pct_calorias_azucar'].apply(clasificar_azucar)
    print("  ✓ pct_calorias_azucar creada")

# Ratio grasa saturada/grasa total
if 'promedio_tsfat' in nhanes_master.columns and 'promedio_ttfat' in nhanes_master.columns:
    nhanes_master['pct_grasa_saturada'] = (
        nhanes_master['promedio_tsfat'] / nhanes_master['promedio_ttfat']
    ) * 100
    print("  ✓ pct_grasa_saturada creada")

# Categoría de fibra (importante para diabetes)
if 'promedio_tfibe' in nhanes_master.columns:
    def clasificar_fibra(valor):
        if pd.isna(valor):
            return np.nan
        elif valor < 15:
            return 'Bajo'
        elif valor < 25:
            return 'Adecuado'
        else:
            return 'Alto'
    
    nhanes_master['categoria_fibra'] = nhanes_master['promedio_tfibe'].apply(clasificar_fibra)
    print("  ✓ categoria_fibra creada")

# ----------------------------------------
# 4.8 VARIABLES PARA COMPARAR CON BRFSS
# ----------------------------------------
print("\n4.8. Variables comparables con BRFSS...")

# Grupo de edad compatible con BRFSS
if 'RIDAGEYR' in nhanes_master.columns:
    def grupo_edad_brfss(edad):
        if pd.isna(edad):
            return np.nan
        elif edad < 18:
            return '<18'
        elif edad < 25:
            return '18-24'
        elif edad < 35:
            return '25-34'
        elif edad < 45:
            return '35-44'
        elif edad < 55:
            return '45-54'
        elif edad < 65:
            return '55-64'
        else:
            return '65+'
    
    nhanes_master['edad_grupo_brfss'] = nhanes_master['RIDAGEYR'].apply(grupo_edad_brfss)
    print("  ✓ edad_grupo_brfss creada")

# ========================================
# 5. GUARDAR TABLAS PROCESADAS
# ========================================
print(f"\n{'='*70}")
print("PASO 5: GUARDANDO TABLAS EN BASE DE DATOS")
print("="*70)

# Guardar tabla maestra
try:
    nhanes_master.to_sql("NHANES_MASTER", conn, if_exists="replace", index=False)
    print(f"✓ NHANES_MASTER guardada: {nhanes_master.shape[0]:,} filas × {nhanes_master.shape[1]} columnas")
except Exception as e:
    print(f"✗ Error guardando NHANES_MASTER: {e}")

# Guardar tablas individuales limpias
print("\nGuardando tablas individuales limpias...")
for tabla, df in nhanes_clean.items():
    try:
        df.to_sql(f"{tabla}_LIMPIO", conn, if_exists="replace", index=False)
        print(f"  ✓ {tabla}_LIMPIO: {df.shape[0]:,} filas × {df.shape[1]} columnas")
    except Exception as e:
        print(f"  ✗ Error en {tabla}_LIMPIO: {e}")

# ========================================
# 6. CREAR ÍNDICES PARA OPTIMIZAR CONSULTAS
# ========================================
print(f"\n{'='*70}")
print("PASO 6: CREANDO ÍNDICES SQL")
print("="*70)

indices = [
    ("idx_nhanes_seqn", "NHANES_MASTER", "SEQN"),
    ("idx_nhanes_diabetes", "NHANES_MASTER", "tiene_diabetes"),
    ("idx_nhanes_colesterol", "NHANES_MASTER", "colesterol_alto"),
    ("idx_nhanes_edad", "NHANES_MASTER", "grupo_edad"),
    ("idx_nhanes_imc", "NHANES_MASTER", "categoria_imc"),
    ("idx_nhanes_riesgo_cv", "NHANES_MASTER", "categoria_riesgo_cv")
]

for idx_name, tabla, columna in indices:
    try:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {tabla}({columna})")
        print(f"  ✓ {idx_name} creado")
    except Exception as e:
        print(f"  ✗ Error creando {idx_name}: {e}")

# ========================================
# 7. REPORTES ESTADÍSTICOS FINALES
# ========================================
print(f"\n{'='*70}")
print("PASO 7: REPORTES ESTADÍSTICOS")
print("="*70)

# Reporte de diabetes
if 'tiene_diabetes' in nhanes_master.columns:
    print("\n REPORTE: DIABETES")
    print("-" * 50)
    
    total = nhanes_master['tiene_diabetes'].notna().sum()
    con_diabetes = nhanes_master['tiene_diabetes'].sum()
    prevalencia = (con_diabetes / total) * 100
    
    print(f"Total participantes con datos: {total:,}")
    print(f"Casos de diabetes: {con_diabetes:.0f}")
    print(f"Prevalencia: {prevalencia:.2f}%")
    
    # Por grupo de edad
    if 'grupo_edad' in nhanes_master.columns:
        print("\nDiabetes por grupo de edad:")
        diabetes_edad = nhanes_master.groupby('grupo_edad')['tiene_diabetes'].agg([
            ('Total', 'count'),
            ('Con_Diabetes', 'sum'),
            ('Prevalencia_%', lambda x: (x.sum() / x.count() * 100))
        ]).round(2)
        print(diabetes_edad)
    
    # Por categoría IMC
    if 'categoria_imc' in nhanes_master.columns:
        print("\nDiabetes por categoría IMC:")
        diabetes_imc = nhanes_master.groupby('categoria_imc')['tiene_diabetes'].agg([
            ('Total', 'count'),
            ('Con_Diabetes', 'sum'),
            ('Prevalencia_%', lambda x: (x.sum() / x.count() * 100))
        ]).round(2)
        print(diabetes_imc)
    
    # Por nivel de ingresos
    if 'categoria_ingreso' in nhanes_master.columns:
        print("\nDiabetes por nivel de ingresos:")
        diabetes_ingreso = nhanes_master.groupby('categoria_ingreso')['tiene_diabetes'].agg([
            ('Total', 'count'),
            ('Con_Diabetes', 'sum'),
            ('Prevalencia_%', lambda x: (x.sum() / x.count() * 100))
        ]).round(2)
        print(diabetes_ingreso)

# Reporte de colesterol
if 'colesterol_alto' in nhanes_master.columns:
    print("\n REPORTE: COLESTEROL")
    print("-" * 50)
    
    total = nhanes_master['colesterol_alto'].notna().sum()
    con_col_alto = nhanes_master['colesterol_alto'].sum()
    prevalencia = (con_col_alto / total) * 100
    
    print(f"Total participantes con datos: {total:,}")
    print(f"Casos de colesterol alto (≥240 mg/dL): {con_col_alto:.0f}")
    print(f"Prevalencia: {prevalencia:.2f}%")
    
    # Distribución de categorías
    if 'categoria_colesterol' in nhanes_master.columns:
        print("\nDistribución de colesterol total:")
        dist_col = nhanes_master['categoria_colesterol'].value_counts()
        print(dist_col)

# Reporte de síndrome metabólico
if 'sindrome_metabolico' in nhanes_master.columns:
    print("\n REPORTE: SÍNDROME METABÓLICO")
    print("-" * 50)
    
    total = len(nhanes_master)
    con_sm = nhanes_master['sindrome_metabolico'].sum()
    prevalencia = (con_sm / total) * 100
    
    print(f"Total participantes: {total:,}")
    print(f"Casos de síndrome metabólico: {con_sm:,}")
    print(f"Prevalencia: {prevalencia:.2f}%")
    
    # Distribución de criterios
    print("\nDistribución de criterios (score 0-4):")
    dist_score = nhanes_master['sm_score'].value_counts().sort_index()
    print(dist_score)

# Reporte de riesgo cardiovascular
if 'categoria_riesgo_cv' in nhanes_master.columns:
    print("\nREPORTE: RIESGO CARDIOVASCULAR")
    print("-" * 50)
    
    print("Distribución por categoría de riesgo:")
    dist_riesgo = nhanes_master['categoria_riesgo_cv'].value_counts()
    print(dist_riesgo)
    
    # Estadísticas del score
    if 'riesgo_cardiovascular' in nhanes_master.columns:
        print(f"\nScore promedio de riesgo CV: {nhanes_master['riesgo_cardiovascular'].mean():.2f}")
        print(f"Mediana: {nhanes_master['riesgo_cardiovascular'].median():.2f}")

# Reporte de variables dietéticas
if 'promedio_tkcal' in nhanes_master.columns:
    print("\n REPORTE: INGESTA DIETÉTICA")
    print("-" * 50)
    
    vars_dieta_reporte = {
        'promedio_tkcal': 'Calorías totales',
        'promedio_tcarb': 'Carbohidratos (g)',
        'promedio_tsugr': 'Azúcares (g)',
        'promedio_tfibe': 'Fibra (g)',
        'promedio_ttfat': 'Grasa total (g)',
        'promedio_tsfat': 'Grasa saturada (g)',
        'promedio_tchol': 'Colesterol dietético (mg)',
        'promedio_tsodi': 'Sodio (mg)',
        'promedio_tprot': 'Proteína (g)'
    }
    
    print("Promedios de ingesta dietética:")
    for var, nombre in vars_dieta_reporte.items():
        if var in nhanes_master.columns:
            media = nhanes_master[var].mean()
            mediana = nhanes_master[var].median()
            print(f"  {nombre}: Media={media:.1f}, Mediana={mediana:.1f}")

# Resumen de completitud de variables clave
print("\n COMPLETITUD DE VARIABLES CLAVE")
print("-" * 50)

vars_clave = [
    'tiene_diabetes', 'colesterol_alto', 'BMXBMI', 'LBXGH', 'LBXGLU',
    'LBXTC', 'LBDHDD', 'promedio_tkcal', 'sindrome_metabolico',
    'riesgo_cardiovascular'
]

print("Porcentaje de datos disponibles:")
for var in vars_clave:
    if var in nhanes_master.columns:
        completitud = (nhanes_master[var].notna().sum() / len(nhanes_master)) * 100
        print(f"  {var}: {completitud:.1f}%")

# ========================================
# FINALIZACIÓN
# ========================================
print(f"\n{'='*70}")
print(" LIMPIEZA Y TRANSFORMACIÓN COMPLETADA")
print("="*70)
print(f"\nTabla principal: NHANES_MASTER")
print(f"Participantes: {nhanes_master.shape[0]:,}")
print(f"Variables: {nhanes_master.shape[1]}")
print(f"\nVariables derivadas clave creadas:")
print("  ✓ tiene_diabetes (HbA1c + glucosa)")
print("  ✓ colesterol_alto (≥240 mg/dL)")
print("  ✓ sindrome_metabolico (criterios ATP III)")
print("  ✓ riesgo_cardiovascular (score 0-10)")
print("  ✓ categoria_imc")
print("  ✓ Variables dietéticas promediadas")
print("  ✓ Variables comparables con BRFSS")
print("\n" + "="*70)

conn.close()
print("\n✓ Conexión a base de datos cerrada")