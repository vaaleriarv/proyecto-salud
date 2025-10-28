import pandas as pd
import numpy as np
import sqlite3

# ========================================
# CONFIGURACIÓN INICIAL
# ========================================
conn = sqlite3.connect("pipeline.db")
df = pd.read_sql("SELECT * FROM BRFSS_2024", conn)
print(f"Tabla BRFSS_2024 cargada: {df.shape[0]:,} filas x {df.shape[1]} columnas")
print("\nColumnas disponibles:")
for col in df.columns:
    print(f"  - {col}")

# ========================================
# FUNCIÓN AUXILIAR DE MAPEO
# ========================================
def mapear_columna(df, col, mapa, reemplazar_nones=True):
    """Mapea valores numéricos a etiquetas legibles"""
    if col in df.columns:
        antes = df[col].notna().sum()
        df[col] = df[col].astype('Int64')
        df[col] = df[col].apply(lambda x: mapa.get(x, np.nan) if pd.notna(x) else np.nan)
        if reemplazar_nones:
            df[col] = df[col].replace(['No sabe / No respondió', 'None'], np.nan)
        despues = df[col].notna().sum()
        print(f" Procesando '{col}': {antes:,} → {despues:,} valores válidos")
    else:
        print(f" Columna '{col}' no encontrada")

# ========================================
# LIMPIEZA INICIAL
# ========================================
print("\n" + "="*60)
print("PASO 1: LIMPIEZA INICIAL")
print("="*60)

# Eliminar columnas con >75% de valores faltantes
threshold_brfss = 0.75
col_nan_pct = df.isnull().sum() / len(df)
cols_to_drop = col_nan_pct[col_nan_pct > threshold_brfss].index.tolist()

brfss_limpio = df.drop(columns=cols_to_drop)

print(f"\n Dimensiones originales: {df.shape[0]:,} filas x {df.shape[1]} columnas")
print(f"  Columnas eliminadas (>75% NaN): {len(cols_to_drop)}")
print(f" Dimensiones después de eliminar columnas: {brfss_limpio.shape[0]:,} filas x {brfss_limpio.shape[1]} columnas")

# Eliminar valores inválidos
print("\n Eliminando valores inválidos...")
valores_invalidos = [-9, 77, 99, 'NA', 'N/A', ' ', '']
brfss_limpio = brfss_limpio.replace(valores_invalidos, np.nan)

# Eliminar filas completamente vacías
original_count = len(brfss_limpio)
brfss_limpio = brfss_limpio.dropna(how='all')
eliminados = original_count - len(brfss_limpio)
print(f"  Filas eliminadas (completamente vacías): {eliminados:,}")

# ========================================
# PASO 2: MAPEO DE VARIABLES DEMOGRÁFICAS
# ========================================
print("\n" + "="*60)
print("PASO 2: MAPEO DE VARIABLES DEMOGRÁFICAS")
print("="*60)

estado_map = {
    1:"Alabama",2:"Alaska",4:"Arizona",5:"Arkansas",6:"California",
    8:"Colorado",9:"Connecticut",10:"Delaware",11:"Distrito de Columbia",
    12:"Florida",13:"Georgia",15:"Hawái",16:"Idaho",17:"Illinois",
    18:"Indiana",19:"Iowa",20:"Kansas",21:"Kentucky",22:"Louisiana",
    23:"Maine",24:"Maryland",25:"Massachusetts",26:"Michigan",
    27:"Minnesota",28:"Mississippi",29:"Missouri",30:"Montana",
    31:"Nebraska",32:"Nevada",33:"New Hampshire",34:"New Jersey",
    35:"New Mexico",36:"New York",37:"North Carolina",38:"North Dakota",
    39:"Ohio",40:"Oklahoma",41:"Oregon",42:"Pennsylvania",
    44:"Rhode Island",45:"South Carolina",46:"South Dakota",
    47:"Tennessee",48:"Texas",49:"Utah",50:"Vermont",
    51:"Virginia",53:"Washington",54:"West Virginia",55:"Wisconsin",
    56:"Wyoming"
}
mapear_columna(brfss_limpio, "_STATE", estado_map)

marital_map = {
    1:'Casado/a',2:'Divorciado/a',3:'Viudo/a',4:'Separado/a',
    5:'Nunca se ha casado',6:'Pareja no casada',9:'No sabe / No respondió'
}
mapear_columna(brfss_limpio, "MARITAL", marital_map)

educag_map = {
    1:'No completó secundaria',
    2:'Graduado de secundaria',
    3:'Asistió a universidad o escuela técnica',
    4:'Graduado de universidad o escuela técnica',
    9:'No sabe / Falta de información'
}
mapear_columna(brfss_limpio, "_EDUCAG", educag_map)

children_map = {
    1:'Sin niños en el hogar',2:'1 niño en el hogar',3:'2 niños en el hogar',
    4:'3 niños en el hogar',5:'4 niños en el hogar',6:'5 o más niños en el hogar',
    9:'No sabe / No respondió'
}
mapear_columna(brfss_limpio, "_CHLDCNT", children_map)

income_map = {
    1:'Menos de $15,000',2:'$15,000 a < $25,000',3:'$25,000 a < $35,000',
    4:'$35,000 a < $50,000',5:'$50,000 a < $100,000',6:'$100,000 a < $200,000',
    7:'$200,000 o más',9:'No sabe / No respondió'
}
mapear_columna(brfss_limpio, "_INCOMG1", income_map)

age_g_map = {1:'18-24',2:'25-34',3:'35-44',4:'45-54',5:'55-64',6:'65+'}
mapear_columna(brfss_limpio, "_AGE_G", age_g_map)

sex_map = {1:'Hombre',2:'Mujer'}
mapear_columna(brfss_limpio, "_SEX", sex_map)

urbstat_map = {1:'Urbano',2:'Rural'}
mapear_columna(brfss_limpio, "_URBSTAT", urbstat_map)

metstat_map = {1:'Condados metropolitanos',2:'Condados no metropolitanos'}
mapear_columna(brfss_limpio, "_METSTAT", metstat_map)

# ========================================
# PASO 3: MAPEO DE VARIABLES DE SALUD
# ========================================
print("\n" + "="*60)
print("PASO 3: MAPEO DE VARIABLES DE SALUD")
print("="*60)

medcost_map = {
    1:'Sí, no pudo pagar',2:'No',
    7:'No sabe / Incertidumbre',9:'Se negó a responder'
}
mapear_columna(brfss_limpio, "MEDCOST1", medcost_map)

checkup_map = {
    1:'En el último año (<12 meses)',2:'Hace 1 a 2 años',3:'Hace 2 a 5 años',
    4:'Hace 5 o más años',7:'No sabe / Incertidumbre',8:'Nunca',9:'Se negó a responder'
}
mapear_columna(brfss_limpio, "CHECKUP1", checkup_map)

# ========================================
# PASO 4: VARIABLES CRÍTICAS PARA DIABETES
# ========================================
print("\n" + "="*60)
print("PASO 4: MAPEO DE VARIABLES DE DIABETES Y FACTORES DE RIESGO")
print("="*60)

# DIABETES (variable más importante)
diabetes_map = {
    1: 'Sí',
    2: 'Sí, solo durante embarazo',
    3: 'No',
    4: 'No, prediabetes',
    7: 'No sabe',
    9: 'Se negó'
}
mapear_columna(brfss_limpio, 'DIABETE4', diabetes_map)

# PRE-DIABETES
prediab_map = {
    1: 'Sí',
    2: 'No', 
    7: 'No sabe',
    9: 'Se negó'
}
mapear_columna(brfss_limpio, 'PREDIAB2', prediab_map)

# TIPO DE DIABETES
diabtype_map = {
    1: 'Tipo 1',
    2: 'Tipo 2',
    3: 'Gestacional',
    4: 'Otro tipo',
    7: 'No sabe',
    9: 'Se negó'
}
mapear_columna(brfss_limpio, 'DIABTYPE', diabtype_map)

# EDAD AL DIAGNÓSTICO
if 'DIABAGE4' in brfss_limpio.columns:
    brfss_limpio['DIABAGE4'] = pd.to_numeric(brfss_limpio['DIABAGE4'], errors='coerce')
    brfss_limpio.loc[brfss_limpio['DIABAGE4'] > 97, 'DIABAGE4'] = np.nan
    print(f"✓ Procesando 'DIABAGE4': edad al diagnóstico limpiada")

# EJERCICIO
ejercicio_map = {1: 'Sí', 2: 'No', 7: 'No sabe', 9: 'Se negó'}
mapear_columna(brfss_limpio, 'EXERANY2', ejercicio_map)

# TABACO
smoker_map = {
    1: 'Fumador actual diario',
    2: 'Fumador actual ocasional',
    3: 'Ex-fumador',
    4: 'Nunca ha fumado',
    9: 'No sabe'
}
mapear_columna(brfss_limpio, '_SMOKER3', smoker_map)

# ALCOHOL - Binge drinking
if 'DRNK3GE5' in brfss_limpio.columns:
    alcohol_map = {1: 'Sí', 2: 'No', 7: 'No sabe', 9: 'Se negó'}
    mapear_columna(brfss_limpio, 'DRNK3GE5', alcohol_map)

# BEBIDAS AZUCARADAS
if 'SSBFRUT3' in brfss_limpio.columns:
    brfss_limpio['SSBFRUT3'] = pd.to_numeric(brfss_limpio['SSBFRUT3'], errors='coerce')
    print(f" Procesando 'SSBFRUT3': consumo de bebidas azucaradas")

# ========================================
# PASO 5: VALIDACIÓN Y LIMPIEZA DE IMC
# ========================================
print("\n" + "="*60)
print("PASO 5: VALIDACIÓN Y LIMPIEZA DE IMC")
print("="*60)

if '_BMI5' in brfss_limpio.columns:
    # Convertir a numérico
    brfss_limpio['_BMI5'] = pd.to_numeric(brfss_limpio['_BMI5'], errors='coerce')
    
    # Crear IMC real (dividir por 100)
    brfss_limpio['IMC_REAL'] = brfss_limpio['_BMI5'] / 100
    
    # Validar rango razonable (10-80)
    inconsistentes = brfss_limpio[(brfss_limpio['IMC_REAL'] < 10) | (brfss_limpio['IMC_REAL'] > 80)]
    print(f"  Registros con IMC fuera de rango (10-80): {len(inconsistentes):,}")
    
    if len(inconsistentes) > 0:
        brfss_limpio.loc[(brfss_limpio['IMC_REAL'] < 10) | (brfss_limpio['IMC_REAL'] > 80), 'IMC_REAL'] = np.nan
        print(f"✓ Valores corregidos (marcados como NaN)")
    
    # Detección de outliers con IQR
    Q1 = brfss_limpio['IMC_REAL'].quantile(0.25)
    Q3 = brfss_limpio['IMC_REAL'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 3 * IQR
    upper_bound = Q3 + 3 * IQR
    
    brfss_limpio['es_outlier_imc'] = (
        (brfss_limpio['IMC_REAL'] < lower_bound) | 
        (brfss_limpio['IMC_REAL'] > upper_bound)
    ).astype(int)
    
    n_outliers = brfss_limpio['es_outlier_imc'].sum()
    pct_outliers = (n_outliers / len(brfss_limpio)) * 100
    print(f" Outliers detectados: {n_outliers:,} ({pct_outliers:.2f}%)")
    print(f"   Rango normal: {lower_bound:.1f} - {upper_bound:.1f}")
    
    # Categorización del IMC
    def clasificar_imc(valor):
        if pd.isna(valor):
            return "Desconocido"
        elif valor < 18.5:
            return "Bajo peso"
        elif valor < 25:
            return "Normal"
        elif valor < 30:
            return "Sobrepeso"
        else:
            return "Obesidad"
    
    brfss_limpio['categoria_IMC'] = brfss_limpio['IMC_REAL'].apply(clasificar_imc)
    
    print("\n Distribución por categoría de IMC:")
    print(brfss_limpio['categoria_IMC'].value_counts().sort_index())

# ========================================
# PASO 6: CREAR VARIABLES DERIVADAS
# ========================================
print("\n" + "="*60)
print("PASO 6: CREACIÓN DE VARIABLES DERIVADAS")
print("="*60)

# 1. DIABETES (variable binaria)
if 'DIABETE4' in brfss_limpio.columns:
    brfss_limpio['tiene_diabetes'] = brfss_limpio['DIABETE4'].apply(
        lambda x: 1 if x in ['Sí', 'Sí, solo durante embarazo'] else 
                 (0 if x == 'No' else np.nan)
    )
    print(f"✓ Variable 'tiene_diabetes' creada")

# 2. RIESGO METABÓLICO
def calcular_riesgo_metabolico(row):
    """Score de riesgo metabólico (0-8)"""
    score = 0
    
    # IMC (+3 obesidad, +2 sobrepeso)
    if pd.notna(row.get('IMC_REAL')):
        if row['IMC_REAL'] >= 30:
            score += 3
        elif row['IMC_REAL'] >= 25:
            score += 2
    
    # Diabetes (+3)
    if row.get('tiene_diabetes') == 1:
        score += 3
    
    # Edad (+2 si 55+, +1 si 45-54)
    if row.get('_AGE_G') in ['55-64', '65+']:
        score += 2
    elif row.get('_AGE_G') == '45-54':
        score += 1
    
    return score

brfss_limpio['riesgo_metabolico'] = brfss_limpio.apply(calcular_riesgo_metabolico, axis=1)

def clasificar_riesgo(score):
    if pd.isna(score):
        return 'Desconocido'
    elif score <= 2:
        return 'Bajo'
    elif score <= 5:
        return 'Moderado'
    else:
        return 'Alto'

brfss_limpio['categoria_riesgo'] = brfss_limpio['riesgo_metabolico'].apply(clasificar_riesgo)
print(f"✓ Variables 'riesgo_metabolico' y 'categoria_riesgo' creadas")

# 3. ESTILO DE VIDA SALUDABLE
def evaluar_estilo_vida(row):
    """1 = saludable, 0 = no saludable"""
    saludable = True
    
    # No hace ejercicio
    if row.get('EXERANY2') == 'No':
        saludable = False
    
    # Fuma actualmente
    if row.get('_SMOKER3') in ['Fumador actual diario', 'Fumador actual ocasional']:
        saludable = False
    
    # Binge drinking
    if row.get('DRNK3GE5') == 'Sí':
        saludable = False
    
    return 1 if saludable else 0

brfss_limpio['estilo_vida_saludable'] = brfss_limpio.apply(evaluar_estilo_vida, axis=1)
print(f" Variable 'estilo_vida_saludable' creada")

# 4. SCORE SOCIOECONÓMICO
def calcular_ses(row):
    """Score de estatus socioeconómico (0-4)"""
    score = 0
    
    # Educación
    if row.get('_EDUCAG') == 'Graduado de universidad o escuela técnica':
        score += 2
    elif row.get('_EDUCAG') == 'Asistió a universidad o escuela técnica':
        score += 1
    
    # Ingresos
    if row.get('_INCOMG1') in ['$100,000 a < $200,000', '$200,000 o más']:
        score += 2
    elif row.get('_INCOMG1') == '$50,000 a < $100,000':
        score += 1
    
    # Acceso a salud (penalización)
    if row.get('MEDCOST1') == 'Sí, no pudo pagar':
        score -= 1
    
    return max(0, score)

brfss_limpio['ses_score'] = brfss_limpio.apply(calcular_ses, axis=1)
print(f" Variable 'ses_score' creada")

# ========================================
# PASO 7: REPORTES Y ESTADÍSTICAS
# ========================================
print("\n" + "="*60)
print("PASO 7: ESTADÍSTICAS FINALES")
print("="*60)

print(f"\n Dimensiones finales: {brfss_limpio.shape[0]:,} filas x {brfss_limpio.shape[1]} columnas")
completitud = (1 - brfss_limpio.isnull().sum().sum() / (brfss_limpio.shape[0] * brfss_limpio.shape[1])) * 100
print(f" Completitud global: {completitud:.2f}%")

# Estadísticas de IMC
if 'IMC_REAL' in brfss_limpio.columns:
    print("\n ESTADÍSTICAS DE IMC:")
    print(f"  Media: {brfss_limpio['IMC_REAL'].mean():.2f}")
    print(f"  Mediana: {brfss_limpio['IMC_REAL'].median():.2f}")
    print(f"  Desviación estándar: {brfss_limpio['IMC_REAL'].std():.2f}")
    print(f"  Mínimo: {brfss_limpio['IMC_REAL'].min():.2f}")
    print(f"  Máximo: {brfss_limpio['IMC_REAL'].max():.2f}")

# ========================================
# REPORTE ESPECÍFICO: DIABETES
# ========================================
if 'tiene_diabetes' in brfss_limpio.columns:
    print("\n" + "="*60)
    print("REPORTE: PREVALENCIA DE DIABETES")
    print("="*60)
    
    total = brfss_limpio['tiene_diabetes'].notna().sum()
    con_diabetes = (brfss_limpio['tiene_diabetes'] == 1).sum()
    prevalencia = (con_diabetes / total) * 100 if total > 0 else 0
    
    print(f"\n PREVALENCIA GENERAL:")
    print(f"  Total casos con diabetes: {con_diabetes:,}")
    print(f"  Total casos válidos: {total:,}")
    print(f"  Prevalencia: {prevalencia:.2f}%")
    
    # Por edad
    if '_AGE_G' in brfss_limpio.columns:
        print(f"\n DIABETES POR GRUPO DE EDAD:")
        diabetes_edad = brfss_limpio.groupby('_AGE_G')['tiene_diabetes'].agg([
            ('Total', 'count'),
            ('Con_Diabetes', 'sum'),
            ('Prevalencia_%', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).round(2)
        print(diabetes_edad)
    
    # Por IMC
    if 'categoria_IMC' in brfss_limpio.columns:
        print(f"\n DIABETES POR CATEGORÍA DE IMC:")
        diabetes_imc = brfss_limpio.groupby('categoria_IMC')['tiene_diabetes'].agg([
            ('Total', 'count'),
            ('Con_Diabetes', 'sum'),
            ('Prevalencia_%', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).round(2)
        print(diabetes_imc)
    
    # Por nivel socioeconómico
    if 'ses_score' in brfss_limpio.columns:
        print(f"\n DIABETES POR NIVEL SOCIOECONÓMICO:")
        diabetes_ses = brfss_limpio.groupby('ses_score')['tiene_diabetes'].agg([
            ('Total', 'count'),
            ('Con_Diabetes', 'sum'),
            ('Prevalencia_%', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).round(2)
        print(diabetes_ses)
    
    # Por estilo de vida
    if 'estilo_vida_saludable' in brfss_limpio.columns:
        print(f"\n DIABETES POR ESTILO DE VIDA:")
        diabetes_estilo = brfss_limpio.groupby('estilo_vida_saludable')['tiene_diabetes'].agg([
            ('Total', 'count'),
            ('Con_Diabetes', 'sum'),
            ('Prevalencia_%', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).round(2)
        diabetes_estilo.index = ['No Saludable', 'Saludable']
        print(diabetes_estilo)

# Resumen de completitud por columna
print("\n" + "="*60)
print("RESUMEN DE COMPLETITUD POR COLUMNA")
print("="*60)
calidad = pd.DataFrame({
    'Columna': brfss_limpio.columns,
    'Tipo': brfss_limpio.dtypes.values,
    'No_Nulos': brfss_limpio.count().values,
    'Nulos': brfss_limpio.isnull().sum().values,
    'Pct_Completo': (brfss_limpio.count() / len(brfss_limpio) * 100).round(2).values
})
calidad = calidad.sort_values('Pct_Completo', ascending=False)
print("\nTop 10 columnas más completas:")
print(calidad.head(10).to_string(index=False))
print("\nTop 10 columnas menos completas:")
print(calidad.tail(10).to_string(index=False))

# ========================================
# PASO 8: GUARDAR EN SQLITE
# ========================================
print("\n" + "="*60)
print("PASO 8: GUARDANDO EN BASE DE DATOS")
print("="*60)

# Guardar tabla limpia con el nombre correcto
brfss_limpio.to_sql("BRFSS_2024_LIMPIO", conn, if_exists="replace", index=False)
print(f" Tabla 'BRFSS_2024_LIMPIO' guardada exitosamente")

# Crear índices para consultas rápidas
print("\n🔧 Creando índices para optimizar consultas...")
cursor = conn.cursor()
indices = [
    "CREATE INDEX IF NOT EXISTS idx_brfss_estado ON BRFSS_2024_LIMPIO(_STATE);",
    "CREATE INDEX IF NOT EXISTS idx_brfss_edad ON BRFSS_2024_LIMPIO(_AGE_G);",
    "CREATE INDEX IF NOT EXISTS idx_brfss_sexo ON BRFSS_2024_LIMPIO(_SEX);",
    "CREATE INDEX IF NOT EXISTS idx_brfss_diabetes ON BRFSS_2024_LIMPIO(tiene_diabetes);",
    "CREATE INDEX IF NOT EXISTS idx_brfss_imc_cat ON BRFSS_2024_LIMPIO(categoria_IMC);"
]

for idx_query in indices:
    try:
        cursor.execute(idx_query)
        print(f"  ✓ {idx_query.split('idx_')[1].split(' ')[0]}")
    except Exception as e:
        print(f"  ✗ Error: {e}")

conn.commit()
conn.close()

print("\n" + "="*60)
print(" LIMPIEZA COMPLETADA EXITOSAMENTE")
print("="*60)
print(f"\n Tabla guardada: BRFSS_2024_LIMPIO")
print(f" Registros finales: {brfss_limpio.shape[0]:,}")
print(f" Columnas finales: {brfss_limpio.shape[1]}")
print(f" Variables derivadas creadas: 6")
print(f"   - tiene_diabetes")
print(f"   - riesgo_metabolico")
print(f"   - categoria_riesgo")
print(f"   - estilo_vida_saludable")
print(f"   - ses_score")
print(f"   - categoria_IMC")
print("\n ¡Listo para análisis!")