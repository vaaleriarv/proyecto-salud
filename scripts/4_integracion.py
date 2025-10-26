import pandas as pd
import sqlite3
import numpy as np
from rapidfuzz import fuzz, process

# CONFIGURACIÓN INICIAL
conn = sqlite3.connect("pipeline.db")
print("="*80)
print("INTEGRACIÓN COMPLETA DE DATASETS - ANÁLISIS DIABETES/COLESTEROL")
print("="*80)

# ============================================================================
# PARTE 1: CREAR TABLA FDC PIVOTEADA CON NUTRIENTES CLAVE
# ============================================================================
print("\n" + "="*80)
print("PARTE 1: PREPARAR TABLA FDC CON NUTRIENTES CLAVE")
print("="*80)

# Definir nutrientes críticos para análisis de diabetes/colesterol
nutrientes_clave = {
    1003: 'Proteina',
    1004: 'Grasa_Total',
    1005: 'Carbohidratos',
    1079: 'Fibra',
    1087: 'Calcio',
    1089: 'Hierro',
    1095: 'Zinc',
    1253: 'Colesterol_Dietetico',
    1258: 'Acidos_Grasos_Saturados',
    1292: 'Acidos_Grasos_Monoinsaturados',
    1293: 'Acidos_Grasos_Poliinsaturados',
    2000: 'Azucares_Totales',
    1008: 'Energia',
    1093: 'Sodio'
}

try:
    # Cargar tablas limpias de FDC
    df_food = pd.read_sql("SELECT * FROM FDC_FOOD_CLEAN", conn)
    df_fn = pd.read_sql("SELECT * FROM FDC_FOOD_NUTRIENT_CLEAN", conn)
    
    print(f"✓ FDC_FOOD_CLEAN cargado: {len(df_food):,} alimentos")
    print(f"✓ FDC_FOOD_NUTRIENT_CLEAN cargado: {len(df_fn):,} registros")
    
    # Filtrar solo nutrientes clave
    df_fn_filtrado = df_fn[df_fn['nutrient_id'].isin(nutrientes_clave.keys())].copy()
    print(f"✓ Nutrientes clave filtrados: {len(df_fn_filtrado):,} registros")
    
    # Pivotar: nutrientes como columnas
    nutrientes_pivot = df_fn_filtrado.pivot_table(
        index='fdc_id',
        columns='nutrient_id',
        values='amount',
        aggfunc='first'
    ).reset_index()
    
    # Renombrar columnas con nombres legibles
    nutrientes_pivot.columns = ['fdc_id'] + [
        nutrientes_clave.get(col, f'nutrient_{col}') 
        for col in nutrientes_pivot.columns[1:]
    ]
    
    # Unir con información del alimento
    fdc_nutrientes = df_food[['fdc_id', 'description', 'food_category_id']].merge(
        nutrientes_pivot, 
        on='fdc_id', 
        how='inner'
    )
    
    # Crear variables derivadas nutricionales
    print("\n📊 Creando variables derivadas nutricionales...")
    
    # Carbohidratos netos (importantes para diabetes)
    fdc_nutrientes['Carb_Netos'] = (
        fdc_nutrientes['Carbohidratos'] - fdc_nutrientes['Fibra'].fillna(0)
    )
    
    # Índice glicémico estimado (aproximación)
    fdc_nutrientes['Indice_Glicemico_Est'] = (
        fdc_nutrientes['Carb_Netos'] / (fdc_nutrientes['Fibra'].fillna(0.1) + 1)
    )
    
    # Grasas saludables vs saturadas
    fdc_nutrientes['Grasas_Saludables'] = (
        fdc_nutrientes['Acidos_Grasos_Monoinsaturados'].fillna(0) + 
        fdc_nutrientes['Acidos_Grasos_Poliinsaturados'].fillna(0)
    )
    
    fdc_nutrientes['Ratio_Grasas_Saludables'] = (
        fdc_nutrientes['Grasas_Saludables'] / 
        (fdc_nutrientes['Acidos_Grasos_Saturados'].fillna(0.1) + 1)
    )
    
    # Densidad de fibra (g fibra / 100 kcal)
    fdc_nutrientes['Densidad_Fibra'] = (
        (fdc_nutrientes['Fibra'].fillna(0) / 
         fdc_nutrientes['Energia'].replace(0, np.nan)) * 100
    )
    
    # Clasificación nutricional básica
    def clasificar_alimento_salud(row):
        """Clasificar alimento según perfil nutricional"""
        # Alto en azúcar
        if row.get('Azucares_Totales', 0) > 15:
            return 'Alto_Azucar'
        # Alto en grasa saturada
        elif row.get('Acidos_Grasos_Saturados', 0) > 5:
            return 'Alto_Grasa_Sat'
        # Alto en sodio
        elif row.get('Sodio', 0) > 400:
            return 'Alto_Sodio'
        # Alto en fibra (saludable)
        elif row.get('Fibra', 0) > 5:
            return 'Alto_Fibra'
        # Perfil balanceado
        else:
            return 'Balanceado'
    
    fdc_nutrientes['clasificacion_salud'] = fdc_nutrientes.apply(
        clasificar_alimento_salud, axis=1
    )
    
    # Guardar tabla integrada FDC
    fdc_nutrientes.to_sql("FDC_NUTRIENTES_INTEGRADO", conn, if_exists="replace", index=False)
    
    print(f" Tabla FDC_NUTRIENTES_INTEGRADO creada: {len(fdc_nutrientes):,} alimentos")
    print(f"   Columnas: {fdc_nutrientes.shape[1]}")
    print("\n Distribución por clasificación de salud:")
    print(fdc_nutrientes['clasificacion_salud'].value_counts())
    
except Exception as e:
    print(f" Error en preparación FDC: {e}")
    fdc_nutrientes = None

# ============================================================================
# PARTE 2: CREAR TABLA ODEPA AGREGADA POR PRODUCTO
# ============================================================================
print("\n" + "="*80)
print("PARTE 2: PREPARAR TABLA ODEPA AGREGADA")
print("="*80)

try:
    df_odepa = pd.read_sql("SELECT * FROM ODEPA_PRECIOS_CLEAN", conn)
    print(f"✓ ODEPA_PRECIOS_CLEAN cargado: {len(df_odepa):,} registros")
    
    # Agregar por producto (promedio de todos los precios)
    odepa_agregado = df_odepa.groupby('Producto').agg({
        'Precio promedio': ['mean', 'min', 'max', 'std', 'count'],
        'Grupo': 'first',
        'Unidad_normalizada': 'first'
    }).reset_index()
    
    # Aplanar columnas multi-nivel
    odepa_agregado.columns = [
        'Producto', 'Precio_Promedio_CLP', 'Precio_Min_CLP', 
        'Precio_Max_CLP', 'Precio_Std_CLP', 'N_Observaciones',
        'Grupo', 'Unidad'
    ]
    
    # Normalizar nombre de producto para matching
    odepa_agregado['producto_normalizado'] = (
        odepa_agregado['Producto']
        .str.lower()
        .str.strip()
        .str.replace(r'[^\w\s]', '', regex=True)  # Eliminar puntuación
        .str.replace(r'\s+', ' ', regex=True)      # Espacios múltiples
    )
    
    # Clasificar por rango de precio (cuartiles)
    odepa_agregado['categoria_precio'] = pd.qcut(
        odepa_agregado['Precio_Promedio_CLP'],
        q=4,
        labels=['Bajo', 'Medio-Bajo', 'Medio-Alto', 'Alto'],
        duplicates='drop'
    )
    
    # Variabilidad de precio
    odepa_agregado['volatilidad_precio'] = (
        odepa_agregado['Precio_Std_CLP'] / odepa_agregado['Precio_Promedio_CLP']
    ) * 100
    
    # Guardar
    odepa_agregado.to_sql("ODEPA_AGREGADO", conn, if_exists="replace", index=False)
    
    print(f" Tabla ODEPA_AGREGADO creada: {len(odepa_agregado):,} productos únicos")
    print(f"\n Distribución por grupo:")
    print(odepa_agregado['Grupo'].value_counts().head(10))
    
except Exception as e:
    print(f" Error en agregación ODEPA: {e}")
    odepa_agregado = None

# ============================================================================
# PARTE 3: FUZZY MATCHING ODEPA ↔ FDC
# ============================================================================
print("\n" + "="*80)
print("PARTE 3: INTEGRACIÓN ODEPA ↔ FDC (FUZZY MATCHING)")
print("="*80)

if fdc_nutrientes is not None and odepa_agregado is not None:
    try:
        print(" Realizando fuzzy matching entre productos ODEPA y FDC...")
        
        # Preparar listas para matching
        fdc_descripciones = fdc_nutrientes['description'].str.lower().tolist()
        
        correspondencias = []
        
        # Para cada producto ODEPA, buscar el mejor match en FDC
        for idx, row in odepa_agregado.iterrows():
            producto_odepa = row['producto_normalizado']
            
        matches = get_close_matches(producto_odepa, fdc_descripciones, n=1, cutoff=0.5)

        if matches:
            fdc_match = matches[0]
            fdc_idx = fdc_descripciones.index(fdc_match)
            fdc_id = fdc_nutrientes.iloc[fdc_idx]['fdc_id']
                
        correspondencias.append({
        'producto_odepa': row['Producto'],
        'producto_odepa_norm': producto_odepa,
        'fdc_id': fdc_id,
        'fdc_description': fdc_match,
        'match_score': np.nan,  # No hay score con difflib
        'precio_promedio_clp': row['Precio_Promedio_CLP'],
        'grupo_odepa': row['Grupo'],
        'categoria_precio': row['categoria_precio']
         })

            
        # Mostrar progreso cada 50 productos
        if (idx + 1) % 50 == 0:
            print(f"   Procesados: {idx + 1}/{len(odepa_agregado)}")
        
        # Crear DataFrame de correspondencias
        df_correspondencias = pd.DataFrame(correspondencias)
        
        # Unir con datos nutricionales de FDC
        odepa_fdc_integrado = df_correspondencias.merge(
            fdc_nutrientes,
            on='fdc_id',
            how='left'
        )
        
        # Guardar tabla integrada
        odepa_fdc_integrado.to_sql(
            "ODEPA_FDC_INTEGRADO", 
            conn, 
            if_exists="replace", 
            index=False
        )
        
        print(f"\n Tabla ODEPA_FDC_INTEGRADO creada")
        print(f"   Total matches: {len(df_correspondencias):,}")
        print(f"   Score promedio: {df_correspondencias['match_score'].mean():.1f}")
        print(f"   Matches con score >80: {(df_correspondencias['match_score'] > 80).sum()}")
        
        # Análisis de precios vs perfil nutricional
        print("\n📊 ANÁLISIS: Precio vs Perfil Nutricional")
        if 'clasificacion_salud' in odepa_fdc_integrado.columns:
            precio_por_salud = odepa_fdc_integrado.groupby('clasificacion_salud').agg({
                'precio_promedio_clp': ['mean', 'median', 'count']
            }).round(2)
            print(precio_por_salud)
        
    except Exception as e:
        print(f" Error en fuzzy matching: {e}")
        odepa_fdc_integrado = None
else:
    print(" Saltando fuzzy matching (faltan tablas previas)")
    odepa_fdc_integrado = None

# ============================================================================
# PARTE 4: ANÁLISIS COMPARATIVO NHANES ↔ BRFSS
# ============================================================================
print("\n" + "="*80)
print("PARTE 4: ANÁLISIS COMPARATIVO NHANES ↔ BRFSS")
print("="*80)

try:
    # Cargar tablas maestras
    nhanes = pd.read_sql("SELECT * FROM NHANES_MASTER", conn)
    brfss = pd.read_sql("SELECT * FROM BRFSS_2024_LIMPIO", conn)
    
    print(f"✓ NHANES_MASTER cargado: {len(nhanes):,} participantes")
    print(f"✓ BRFSS_2024_LIMPIO cargado: {len(brfss):,} participantes")
    
    # -------------------------------------------------------------------
    # 4.1: PREVALENCIAS POR GRUPO DE EDAD (AGREGADO)
    # -------------------------------------------------------------------
    print("\n 4.1. Comparación de prevalencias por grupo de edad")
    
    # NHANES - Prevalencia de diabetes por edad
    if 'edad_grupo_brfss' in nhanes.columns and 'tiene_diabetes' in nhanes.columns:
        nhanes_prev_edad = nhanes.groupby('edad_grupo_brfss')['tiene_diabetes'].agg([
            ('total', 'count'),
            ('con_diabetes', 'sum'),
            ('prevalencia_pct', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).reset_index()
        nhanes_prev_edad['fuente'] = 'NHANES'
        
        print("\nNHANES - Diabetes por edad:")
        print(nhanes_prev_edad)
    
    # BRFSS - Prevalencia de diabetes por edad
    if '_AGE_G' in brfss.columns and 'tiene_diabetes' in brfss.columns:
        brfss_prev_edad = brfss.groupby('_AGE_G')['tiene_diabetes'].agg([
            ('total', 'count'),
            ('con_diabetes', 'sum'),
            ('prevalencia_pct', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).reset_index()
        brfss_prev_edad.columns = ['edad_grupo_brfss', 'total', 'con_diabetes', 'prevalencia_pct']
        brfss_prev_edad['fuente'] = 'BRFSS'
        
        print("\nBRFSS - Diabetes por edad:")
        print(brfss_prev_edad)
    
    # Combinar ambas para comparación
    if 'nhanes_prev_edad' in locals() and 'brfss_prev_edad' in locals():
        comparacion_edad = pd.concat([nhanes_prev_edad, brfss_prev_edad], ignore_index=True)
        comparacion_edad.to_sql("COMPARACION_DIABETES_EDAD", conn, if_exists="replace", index=False)
        print("\n Tabla COMPARACION_DIABETES_EDAD creada")
    
    # -------------------------------------------------------------------
    # 4.2: PREVALENCIAS POR CATEGORÍA IMC
    # -------------------------------------------------------------------
    print("\n 4.2. Comparación de prevalencias por IMC")
    
    # NHANES
    if 'categoria_imc' in nhanes.columns and 'tiene_diabetes' in nhanes.columns:
        nhanes_prev_imc = nhanes.groupby('categoria_imc')['tiene_diabetes'].agg([
            ('total', 'count'),
            ('con_diabetes', 'sum'),
            ('prevalencia_pct', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).reset_index()
        nhanes_prev_imc['fuente'] = 'NHANES'
        
        print("\nNHANES - Diabetes por IMC:")
        print(nhanes_prev_imc)
    
    # BRFSS
    if 'categoria_IMC' in brfss.columns and 'tiene_diabetes' in brfss.columns:
        brfss_prev_imc = brfss.groupby('categoria_IMC')['tiene_diabetes'].agg([
            ('total', 'count'),
            ('con_diabetes', 'sum'),
            ('prevalencia_pct', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).reset_index()
        brfss_prev_imc.columns = ['categoria_imc', 'total', 'con_diabetes', 'prevalencia_pct']
        brfss_prev_imc['fuente'] = 'BRFSS'
        
        print("\nBRFSS - Diabetes por IMC:")
        print(brfss_prev_imc)
    
    # Combinar
    if 'nhanes_prev_imc' in locals() and 'brfss_prev_imc' in locals():
        comparacion_imc = pd.concat([nhanes_prev_imc, brfss_prev_imc], ignore_index=True)
        comparacion_imc.to_sql("COMPARACION_DIABETES_IMC", conn, if_exists="replace", index=False)
        print("\n Tabla COMPARACION_DIABETES_IMC creada")
    
    # -------------------------------------------------------------------
    # 4.3: FACTORES DE RIESGO CONDUCTUALES (BRFSS) vs LABORATORIO (NHANES)
    # -------------------------------------------------------------------
    print("\n📊 4.3. Factores de riesgo: Conductuales vs Laboratorio")
    
    # BRFSS - Prevalencia por estilo de vida
    if 'estilo_vida_saludable' in brfss.columns and 'tiene_diabetes' in brfss.columns:
        brfss_estilo = brfss.groupby('estilo_vida_saludable')['tiene_diabetes'].agg([
            ('total', 'count'),
            ('con_diabetes', 'sum'),
            ('prevalencia_pct', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).reset_index()
        brfss_estilo['estilo_vida_saludable'] = brfss_estilo['estilo_vida_saludable'].map({
            0: 'No Saludable', 1: 'Saludable'
        })
        
        print("\nBRFSS - Diabetes por estilo de vida:")
        print(brfss_estilo)
    
    # NHANES - Prevalencia por ingesta de fibra (factor protector)
    if 'categoria_fibra' in nhanes.columns and 'tiene_diabetes' in nhanes.columns:
        nhanes_fibra = nhanes.groupby('categoria_fibra')['tiene_diabetes'].agg([
            ('total', 'count'),
            ('con_diabetes', 'sum'),
            ('prevalencia_pct', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).reset_index()
        
        print("\nNHANES - Diabetes por consumo de fibra:")
        print(nhanes_fibra)
        
        nhanes_fibra.to_sql("NHANES_DIABETES_FIBRA", conn, if_exists="replace", index=False)
    
    # NHANES - Prevalencia por consumo de azúcar
    if 'categoria_azucar' in nhanes.columns and 'tiene_diabetes' in nhanes.columns:
        nhanes_azucar = nhanes.groupby('categoria_azucar')['tiene_diabetes'].agg([
            ('total', 'count'),
            ('con_diabetes', 'sum'),
            ('prevalencia_pct', lambda x: (x.sum() / x.count() * 100) if x.count() > 0 else 0)
        ]).reset_index()
        
        print("\nNHANES - Diabetes por consumo de azúcar:")
        print(nhanes_azucar)
        
        nhanes_azucar.to_sql("NHANES_DIABETES_AZUCAR", conn, if_exists="replace", index=False)
    
    # -------------------------------------------------------------------
    # 4.4: RESUMEN COMPARATIVO GENERAL
    # -------------------------------------------------------------------
    print("\n 4.4. Resumen comparativo general")
    
    resumen_comparativo = {
        'dataset': ['NHANES', 'BRFSS'],
        'n_participantes': [len(nhanes), len(brfss)],
        'prevalencia_diabetes_pct': [
            (nhanes['tiene_diabetes'].sum() / nhanes['tiene_diabetes'].notna().sum() * 100) 
            if 'tiene_diabetes' in nhanes.columns else np.nan,
            (brfss['tiene_diabetes'].sum() / brfss['tiene_diabetes'].notna().sum() * 100)
            if 'tiene_diabetes' in brfss.columns else np.nan
        ],
        'imc_promedio': [
            nhanes['BMXBMI'].mean() if 'BMXBMI' in nhanes.columns else np.nan,
            (brfss['IMC_REAL'].mean() if 'IMC_REAL' in brfss.columns else np.nan)
        ],
        'edad_promedio': [
            nhanes['RIDAGEYR'].mean() if 'RIDAGEYR' in nhanes.columns else np.nan,
            np.nan  # BRFSS solo tiene grupos
        ]
    }
    
    df_resumen = pd.DataFrame(resumen_comparativo)
    print("\nRESUMEN COMPARATIVO NHANES vs BRFSS:")
    print(df_resumen)
    
    df_resumen.to_sql("RESUMEN_COMPARATIVO_NHANES_BRFSS", conn, if_exists="replace", index=False)
    print("\n Tabla RESUMEN_COMPARATIVO_NHANES_BRFSS creada")
    
except Exception as e:
    print(f" Error en análisis comparativo: {e}")

# ============================================================================
# PARTE 5: TABLA FINAL INTEGRADA PARA ANÁLISIS
# ============================================================================
print("\n" + "="*80)
print("PARTE 5: CREAR TABLA MAESTRA PARA ANÁLISIS")
print("="*80)

try:
    # Seleccionar variables clave de NHANES
    vars_nhanes = [
        'SEQN', 'RIDAGEYR', 'RIAGENDR', 'RIDRETH3', 'DMDEDUC2', 'INDFMPIR',
        'BMXBMI', 'BMXWAIST', 'categoria_imc', 'ratio_cintura_cadera',
        'LBXGH', 'LBXGLU', 'LBXTC', 'LBDHDD', 'LBDLDL_calc',
        'tiene_diabetes', 'diabetes_hba1c', 'diabetes_glucosa',
        'colesterol_alto', 'hdl_bajo', 'trigliceridos_altos',
        'sindrome_metabolico', 'sm_score', 'riesgo_cardiovascular', 'categoria_riesgo_cv',
        'promedio_tkcal', 'promedio_tcarb', 'promedio_tsugr', 'promedio_tfibe',
        'promedio_ttfat', 'promedio_tsfat', 'promedio_tchol', 'promedio_tsodi',
        'pct_calorias_azucar', 'categoria_azucar', 'categoria_fibra',
        'grupo_edad', 'categoria_ingreso', 'edad_grupo_brfss'
    ]
    
    # Filtrar solo columnas existentes
    vars_nhanes_existentes = [v for v in vars_nhanes if v in nhanes.columns]
    
    nhanes_analisis = nhanes[vars_nhanes_existentes].copy()
    nhanes_analisis['fuente_datos'] = 'NHANES'
    
    # Guardar tabla final
    nhanes_analisis.to_sql("DATOS_ANALISIS_FINAL", conn, if_exists="replace", index=False)
    
    print(f" Tabla DATOS_ANALISIS_FINAL creada")
    print(f"   Participantes: {len(nhanes_analisis):,}")
    print(f"   Variables: {len(vars_nhanes_existentes)}")
    
    # Estadísticas de completitud
    print("\n Completitud de variables clave:")
    vars_criticas = [
        'tiene_diabetes', 'colesterol_alto', 'BMXBMI', 'LBXGH', 
        'promedio_tkcal', 'sindrome_metabolico'
    ]
    
    for var in vars_criticas:
        if var in nhanes_analisis.columns:
            completitud = (nhanes_analisis[var].notna().sum() / len(nhanes_analisis)) * 100
            print(f"   {var}: {completitud:.1f}%")
    
except Exception as e:
    print(f" Error creando tabla final: {e}")

# ============================================================================
# PARTE 6: CREAR ÍNDICES PARA OPTIMIZAR CONSULTAS
# ============================================================================
print("\n" + "="*80)
print("PARTE 6: CREACIÓN DE ÍNDICES SQL")
print("="*80)

indices = [
    # FDC
    ("idx_fdc_nutrientes_id", "FDC_NUTRIENTES_INTEGRADO", "fdc_id"),
    ("idx_fdc_clasificacion", "FDC_NUTRIENTES_INTEGRADO", "clasificacion_salud"),
    
    # ODEPA
    ("idx_odepa_producto", "ODEPA_AGREGADO", "Producto"),
    ("idx_odepa_grupo", "ODEPA_AGREGADO", "Grupo"),
    
    # ODEPA-FDC
    ("idx_odepa_fdc_producto", "ODEPA_FDC_INTEGRADO", "producto_odepa"),
    ("idx_odepa_fdc_id", "ODEPA_FDC_INTEGRADO", "fdc_id"),
    
    # Análisis final
    ("idx_analisis_seqn", "DATOS_ANALISIS_FINAL", "SEQN"),
    ("idx_analisis_diabetes", "DATOS_ANALISIS_FINAL", "tiene_diabetes"),
    ("idx_analisis_edad", "DATOS_ANALISIS_FINAL", "grupo_edad")
]

for idx_name, tabla, columna in indices:
    try:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {tabla}({columna})")
        print(f"  ✓ {idx_name}")
    except Exception as e:
        print(f"  ✗ Error en {idx_name}: {e}")

conn.commit()

# ============================================================================
# PARTE 7: REPORTE FINAL DE INTEGRACIÓN
# ============================================================================
print("\n" + "="*80)
print("REPORTE FINAL DE INTEGRACIÓN")
print("="*80)

cursor = conn.cursor()

# Listar todas las tablas creadas
tablas_integradas = [
    'FDC_NUTRIENTES_INTEGRADO',
    'ODEPA_AGREGADO',
    'ODEPA_FDC_INTEGRADO',
    'COMPARACION_DIABETES_EDAD',
    'COMPARACION_DIABETES_IMC',
    'NHANES_DIABETES_FIBRA',
    'NHANES_DIABETES_AZUCAR',
    'RESUMEN_COMPARATIVO_NHANES_BRFSS',
    'DATOS_ANALISIS_FINAL'
]

print("\n📊 TABLAS INTEGRADAS CREADAS:")
for tabla in tablas_integradas:
    try:
        count = cursor.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
        print(f"  ✓ {tabla}: {count:,} registros")
    except:
        print(f"  ⚠ {tabla}: No creada")

print("\n" + "="*80)
print(" INTEGRACIÓN COMPLETADA EXITOSAMENTE")
print("="*80)

print("\n📋 TABLAS PRINCIPALES PARA ANÁLISIS:")
print("  1. DATOS_ANALISIS_FINAL - Dataset master NHANES completo")
print("  2. ODEPA_FDC_INTEGRADO - Precios Chile + Nutrientes EE.UU.")
print("  3. COMPARACION_DIABETES_* - Comparaciones NHANES vs BRFSS")
print("  4. FDC_NUTRIENTES_INTEGRADO - Base nutricional completa")
print("\n Pipeline completo: INGESTA → LIMPIEZA → TRANSFORMACIÓN → INTEGRACIÓN ")

conn.close()
print("\n✓ Conexión a base de datos cerrada")
print("\n" + "="*80)