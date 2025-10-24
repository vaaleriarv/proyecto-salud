import pandas as pd
import sqlite3
import numpy as np

conn = sqlite3.connect("pipeline.db")

# ----------------------------------------------------------------------------
# Cargar datos
# ----------------------------------------------------------------------------

print("\n1. Carga de Datos ODEPA")

df = pd.read_sql("SELECT * FROM ODEPA_PRECIOS", conn)
print(f"Dimensiones originales: {df.shape[0]:,} filas x {df.shape[1]} columnas")
print(f"\nColumnas disponibles:")
for col in df.columns:
    print(f"  - {col}")

# ----------------------------------------------------------------------------
# Inspección inicial
# ----------------------------------------------------------------------------

print("\n2. Inspección de Tipos de Datos")

print(df.dtypes)
print("\nPrimeras 5 filas:")
print(df.head())

# ----------------------------------------------------------------------------
# Limpieza de precios (formato chileno con coma decimal)
# ----------------------------------------------------------------------------

print("\n3. Limpieza de Columnas de Precios")

columnas_precio = ['Precio minimo', 'Precio maximo', 'Precio promedio']

for col in columnas_precio:
    if col in df.columns:
        print(f"\nProcesando '{col}'...")
        
        # Verificar tipo actual
        print(f"  Tipo original: {df[col].dtype}")
        print(f"  Ejemplo de valores: {df[col].head(3).tolist()}")
        
        # Convertir a string primero
        df[col] = df[col].astype(str)
        
        # Reemplazar coma por punto (formato decimal chileno)
        df[col] = df[col].str.replace(',', '.', regex=False)
        
        # Eliminar espacios en blanco
        df[col] = df[col].str.strip()
        
        # Eliminar valores no numéricos como 'nan', 'None', etc.
        df[col] = df[col].replace(['nan', 'None', ''], np.nan)
        
        # Convertir a float
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Estadísticas
        valid_count = df[col].notna().sum()
        invalid_count = df[col].isna().sum()
        print(f"  Valores válidos: {valid_count:,}")
        print(f"  Valores inválidos: {invalid_count:,}")
        
        if valid_count > 0:
            print(f"  Rango: ${df[col].min():.2f} - ${df[col].max():.2f}")
            print(f"  Media: ${df[col].mean():.2f}")

# ----------------------------------------------------------------------------
# Limpieza de columnas temporales
# ----------------------------------------------------------------------------

print("\n4. Procesamiento de Columnas Temporales")

# Anio, Mes, Semana
columnas_temporales_numericas = ['Anio', 'Mes', 'Semana', 'ID region']

for col in columnas_temporales_numericas:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        print(f"  {col}: convertido a numérico")

# Fechas
columnas_fecha = ['Fecha inicio', 'Fecha termino']

for col in columnas_fecha:
    if col in df.columns:
        # Convertir a datetime
        df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d')
        valid_dates = df[col].notna().sum()
        print(f"  {col}: {valid_dates:,} fechas válidas")

# Extraer año y mes para análisis
if 'Fecha inicio' in df.columns and df['Fecha inicio'].notna().any():
    df['año_fecha'] = df['Fecha inicio'].dt.year
    df['mes_fecha'] = df['Fecha inicio'].dt.month
    df['semana_año'] = df['Fecha inicio'].dt.isocalendar().week
    print("  Variables temporales derivadas creadas: año_fecha, mes_fecha, semana_año")

# Agrupar por producto y calcular estadísticas
odepa_precios = df.groupby('Producto').agg({
    'Precio promedio': ['mean', 'min', 'max', 'std'],
    'Region': 'count'
}).reset_index()

odepa_precios.columns = ['Producto', 'Precio_Promedio_CLP', 'Precio_Min_CLP',
                          'Precio_Max_CLP', 'Precio_Std_CLP', 'N_Observaciones']

# Agregar grupo de alimento
grupos = df.groupby('Producto')['Grupo'].first().reset_index()
odepa_precios = odepa_precios.merge(grupos, on='Producto', how='left')


# ----------------------------------------------------------------------------
# Normalización de columnas de texto
# ----------------------------------------------------------------------------

print("\n5. Normalización de Texto")

columnas_texto = ['Region', 'Sector', 'Tipo de punto monitoreo', 'Grupo', 'Producto', 'Unidad']

for col in columnas_texto:
    if col in df.columns:
        # Convertir a string y normalizar
        df[col] = df[col].astype(str)
        df[col] = df[col].str.strip()
        
        # No cambiar a Title case para mantener formato original
        # Solo limpiar espacios extra
        df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
        
        valores_unicos = df[col].nunique()
        print(f"  {col}: {valores_unicos:,} valores únicos")

# ----------------------------------------------------------------------------
# Eliminación de registros inválidos
# ----------------------------------------------------------------------------

print("\n6. Eliminación de Registros Inválidos")

original_count = len(df)

# Eliminar registros sin precio promedio
if 'Precio promedio' in df.columns:
    antes = len(df)
    df = df[df['Precio promedio'].notna()]
    eliminados = antes - len(df)
    if eliminados > 0:
        print(f"  Registros sin precio promedio: {eliminados:,} eliminados")

# Eliminar precios negativos o cero
if 'Precio promedio' in df.columns:
    antes = len(df)
    df = df[df['Precio promedio'] > 0]
    eliminados = antes - len(df)
    if eliminados > 0:
        print(f"  Registros con precio <= 0: {eliminados:,} eliminados")

# Eliminar registros sin producto
if 'Producto' in df.columns:
    antes = len(df)
    df = df[df['Producto'].notna()]
    df = df[df['Producto'] != '']
    df = df[df['Producto'] != 'nan']
    eliminados = antes - len(df)
    if eliminados > 0:
        print(f"  Registros sin producto: {eliminados:,} eliminados")

# Eliminar registros sin grupo
if 'Grupo' in df.columns:
    antes = len(df)
    df = df[df['Grupo'].notna()]
    df = df[df['Grupo'] != '']
    df = df[df['Grupo'] != 'nan']
    eliminados = antes - len(df)
    if eliminados > 0:
        print(f"  Registros sin grupo: {eliminados:,} eliminados")

print(f"\nTotal de registros eliminados: {original_count - len(df):,}")

# ----------------------------------------------------------------------------
# Validación de consistencia de precios
# ----------------------------------------------------------------------------

print("\n7. Validación de Consistencia de Precios")

if all(col in df.columns for col in ['Precio minimo', 'Precio maximo', 'Precio promedio']):
    # Verificar que min <= promedio <= max
    inconsistentes = df[
        (df['Precio promedio'] < df['Precio minimo']) | 
        (df['Precio promedio'] > df['Precio maximo'])
    ]
    
    print(f"Registros con inconsistencia de precios: {len(inconsistentes):,}")
    
    if len(inconsistentes) > 0:
        print("Ejemplos de inconsistencias:")
        print(inconsistentes[['Producto', 'Precio minimo', 'Precio promedio', 'Precio maximo']].head(3))
        
        # Corregir: establecer promedio como media de min y max si está fuera de rango
        print("Corrigiendo inconsistencias...")
        mask = (df['Precio promedio'] < df['Precio minimo']) | (df['Precio promedio'] > df['Precio maximo'])
        df.loc[mask, 'Precio promedio'] = (df.loc[mask, 'Precio minimo'] + df.loc[mask, 'Precio maximo']) / 2
        print(f"  {mask.sum():,} registros corregidos")

# ----------------------------------------------------------------------------
# Detección de outliers
# ----------------------------------------------------------------------------

print("\n8. Detección de Outliers en Precios")

if 'Precio promedio' in df.columns:
    Q1 = df['Precio promedio'].quantile(0.25)
    Q3 = df['Precio promedio'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 3 * IQR
    upper_bound = Q3 + 3 * IQR
    
    # Identificar outliers
    df['es_outlier'] = (
        (df['Precio promedio'] < lower_bound) | 
        (df['Precio promedio'] > upper_bound)
    ).astype(int)
    
    n_outliers = df['es_outlier'].sum()
    pct_outliers = (n_outliers / len(df)) * 100
    
    print(f"Outliers detectados: {n_outliers:,} ({pct_outliers:.2f}%)")
    print(f"Rango normal (Q1-3*IQR, Q3+3*IQR): ${lower_bound:.0f} - ${upper_bound:.0f}")
    
    if n_outliers > 0:
        print("\nProductos con outliers más frecuentes:")
        outliers_por_producto = df[df['es_outlier'] == 1].groupby('Producto').size().sort_values(ascending=False).head(10)
        print(outliers_por_producto)

# ----------------------------------------------------------------------------
# Crear categorías de precio
# ----------------------------------------------------------------------------

print("\n9. Categorización de Precios")

if 'Precio promedio' in df.columns:
    # Crear cuartiles de precio
    df['categoria_precio'] = pd.qcut(
        df['Precio promedio'], 
        q=4, 
        labels=['Bajo', 'Medio-Bajo', 'Medio-Alto', 'Alto'],
        duplicates='drop'
    )
    
    print("Distribución por categoría de precio:")
    print(df['categoria_precio'].value_counts().sort_index())

# ----------------------------------------------------------------------------
# Validación de unidades
# ----------------------------------------------------------------------------

print("\n10. Validación de Unidades")

if 'Unidad' in df.columns:
    unidades_unicas = df['Unidad'].value_counts()
    print("Unidades encontradas:")
    print(unidades_unicas)
    
    # Normalizar unidades comunes
    unidad_map = {
        '$/kilo': 'CLP/kg',
        '$/kilogramo': 'CLP/kg',
        '$/kg': 'CLP/kg',
        '$/litro': 'CLP/L',
        '$/lt': 'CLP/L',
        '$/unidad': 'CLP/unidad',
        '$/un': 'CLP/unidad',
        '$/docena': 'CLP/docena'
    }
    
    df['Unidad_normalizada'] = df['Unidad'].replace(unidad_map)
    print("\nUnidades después de normalización:")
    print(df['Unidad_normalizada'].value_counts())

# ----------------------------------------------------------------------------
# Estadísticas por grupo
# ----------------------------------------------------------------------------

print("\n11. Estadísticas por Grupo de Alimentos")

if 'Grupo' in df.columns and 'Precio promedio' in df.columns:
    resumen_grupos = df.groupby('Grupo').agg({
        'Producto': 'nunique',
        'Precio promedio': ['mean', 'median', 'std', 'min', 'max', 'count']
    }).round(2)
    
    resumen_grupos.columns = ['Productos_Unicos', 'Precio_Medio', 'Precio_Mediana', 
                               'Desv_Std', 'Precio_Min', 'Precio_Max', 'N_Observaciones']
    
    # Ordenar por precio medio descendente
    resumen_grupos = resumen_grupos.sort_values('Precio_Medio', ascending=False)
    
    print(resumen_grupos)

# ----------------------------------------------------------------------------
# Estadísticas por región
# ----------------------------------------------------------------------------

print("\n12. Estadísticas por Región")

if 'Region' in df.columns and 'Precio promedio' in df.columns:
    resumen_regiones = df.groupby('Region').agg({
        'Producto': 'nunique',
        'Precio promedio': ['mean', 'median', 'count']
    }).round(2)
    
    resumen_regiones.columns = ['Productos_Unicos', 'Precio_Medio', 'Precio_Mediana', 'N_Observaciones']
    
    # Ordenar por precio medio descendente
    resumen_regiones = resumen_regiones.sort_values('Precio_Medio', ascending=False)
    
    print(resumen_regiones)

# ----------------------------------------------------------------------------
# Estadísticas por tipo de establecimiento
# ----------------------------------------------------------------------------

print("\n13. Estadísticas por Tipo de Establecimiento")

if 'Tipo de punto monitoreo' in df.columns and 'Precio promedio' in df.columns:
    resumen_establecimientos = df.groupby('Tipo de punto monitoreo').agg({
        'Producto': 'nunique',
        'Precio promedio': ['mean', 'median', 'count']
    }).round(2)
    
    resumen_establecimientos.columns = ['Productos_Unicos', 'Precio_Medio', 'Precio_Mediana', 'N_Observaciones']
    
    print(resumen_establecimientos.sort_values('Precio_Medio'))

# ----------------------------------------------------------------------------
# Eliminar columnas innecesarias
# ----------------------------------------------------------------------------

print("\n14. Eliminación de Columnas Innecesarias")

columnas_antes = df.shape[1]

# Eliminar columnas completamente vacías
df = df.dropna(axis=1, how='all')

# Eliminar columnas redundantes si existen
columnas_redundantes = []
for col in df.columns:
    if df[col].nunique() == 1 and col not in ['Unidad', 'Unidad_normalizada']:
        columnas_redundantes.append(col)

if columnas_redundantes:
    print(f"Columnas con un solo valor (redundantes): {columnas_redundantes}")
    df = df.drop(columns=columnas_redundantes)

columnas_despues = df.shape[1]
print(f"Columnas eliminadas: {columnas_antes - columnas_despues}")

# ----------------------------------------------------------------------------
# Estadísticas finales
# ----------------------------------------------------------------------------

print("\n15. Estadísticas Finales")

print(f"Dimensiones finales: {df.shape[0]:,} filas x {df.shape[1]} columnas")
print(f"Completitud: {(1 - df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100:.2f}%")

if 'Precio promedio' in df.columns:
    print("\nEstadísticas de Precio Promedio (CLP):")
    print(f"  Media: ${df['Precio promedio'].mean():,.2f}")
    print(f"  Mediana: ${df['Precio promedio'].median():,.2f}")
    print(f"  Mínimo: ${df['Precio promedio'].min():,.2f}")
    print(f"  Máximo: ${df['Precio promedio'].max():,.2f}")
    print(f"  Desviación estándar: ${df['Precio promedio'].std():,.2f}")
    print(f"  Rango intercuartílico (IQR): ${df['Precio promedio'].quantile(0.75) - df['Precio promedio'].quantile(0.25):,.2f}")

print(f"\nProductos únicos: {df['Producto'].nunique():,}")
print(f"Grupos únicos: {df['Grupo'].nunique()}")
print(f"Regiones únicas: {df['Region'].nunique()}")

if 'Tipo de punto monitoreo' in df.columns:
    print(f"Tipos de establecimiento: {df['Tipo de punto monitoreo'].nunique()}")

# ----------------------------------------------------------------------------
# Guardar tabla limpia
# ----------------------------------------------------------------------------

print("\n16. Guardando Tabla Limpia")

df.to_sql("ODEPA_PRECIOS_CLEAN", conn, if_exists="replace", index=False)
print("Tabla ODEPA_PRECIOS_CLEAN creada exitosamente")

# Crear también una vista con solo datos recientes (último año)
if 'Anio' in df.columns:
    año_max = df['Anio'].max()
    df_reciente = df[df['Anio'] == año_max]
    df_reciente.to_sql("ODEPA_PRECIOS_RECIENTES", conn, if_exists="replace", index=False)
    print(f"Tabla ODEPA_PRECIOS_RECIENTES creada con datos de {año_max}: {len(df_reciente):,} registros")

# ----------------------------------------------------------------------------
# Crear índices para optimizar consultas
# ----------------------------------------------------------------------------

print("\n17. Creación de Índices")

cursor = conn.cursor()

indices = [
    "CREATE INDEX IF NOT EXISTS idx_odepa_producto ON ODEPA_PRECIOS_CLEAN(Producto);",
    "CREATE INDEX IF NOT EXISTS idx_odepa_grupo ON ODEPA_PRECIOS_CLEAN(Grupo);",
    "CREATE INDEX IF NOT EXISTS idx_odepa_region ON ODEPA_PRECIOS_CLEAN(Region);",
    "CREATE INDEX IF NOT EXISTS idx_odepa_anio ON ODEPA_PRECIOS_CLEAN(Anio);",
    "CREATE INDEX IF NOT EXISTS idx_odepa_precio ON ODEPA_PRECIOS_CLEAN([Precio promedio]);"
]

for idx_query in indices:
    try:
        cursor.execute(idx_query)
        print(f"  Índice creado correctamente")
    except Exception as e:
        print(f"  Error creando índice: {e}")

conn.commit()

# ----------------------------------------------------------------------------
# Resumen de calidad de datos
# ----------------------------------------------------------------------------

print("\n18. Resumen de Calidad de Datos")


calidad = pd.DataFrame({
    'Columna': df.columns,
    'Tipo': df.dtypes.values,
    'No_Nulos': df.count().values,
    'Nulos': df.isnull().sum().values,
    'Pct_Completo': (df.count() / len(df) * 100).round(2).values
})

calidad = calidad.sort_values('Pct_Completo', ascending=False)
print(calidad.to_string(index=False))

conn.close()

print("\n" + "=" * 80)
print("LIMPIEZA ODEPA COMPLETADA")
print("=" * 80)