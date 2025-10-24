import pandas as pd
import numpy as np
import sqlite3
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, LabelEncoder

# ====================================================================================
# CONFIGURACIÓN INICIAL Y CONEXIÓN A BASE DE DATOS
# ====================================================================================

print("\n" + "=" * 80)
print("TRANSFORMACIONES FOODDATA CENTRAL (FDC)")
print("=" * 80)

# Verificar/crear conexión SQLite
try:
    conn
    print("✓ Conexión SQLite existente detectada")
except NameError:
    conn = sqlite3.connect("pipeline.db")
    print("🔗 Conexión SQLite creada: pipeline.db")

# Verificar que la conexión esté activa
try:
    conn.execute("SELECT 1")
    print("✓ Conexión SQLite activa")
except:
    print("⚠️ Reconectando a la base de datos...")
    conn = sqlite3.connect("pipeline.db")
    print("✓ Conexión restablecida")

# ====================================================================================
# PARTE 1: CONVERSIÓN DE TIPOS FDC_FOOD
# ====================================================================================

print("\n1. Conversión de Tipos en FDC_FOOD")

try:
    df_food = pd.read_sql("SELECT * FROM FDC_FOOD_CLEAN", conn)
    print(f"  Dimensiones originales: {df_food.shape}")
    
    # Conversiones de tipo
    df_food['fdc_id'] = df_food['fdc_id'].astype('int32')
    df_food['data_type'] = df_food['data_type'].astype('category')
    df_food['description'] = df_food['description'].astype(str).str.strip()
    
    # food_category_id está como float64, convertir a int (manejando NaN)
    if 'food_category_id' in df_food.columns:
        df_food['food_category_id'] = pd.to_numeric(df_food['food_category_id'], errors='coerce').fillna(0).astype('int16')
    
    # publication_date a datetime
    if 'publication_date' in df_food.columns:
        df_food['publication_date'] = pd.to_datetime(df_food['publication_date'], errors='coerce')
        df_food['publication_year'] = df_food['publication_date'].dt.year
        df_food['publication_month'] = df_food['publication_date'].dt.month
        print("  ✓ publication_year y publication_month creados")
    
    df_food.to_sql("FDC_FOOD_TRANSFORM", conn, if_exists="replace", index=False)
    print(f"  ✓ FDC_FOOD_TRANSFORM creada: {df_food.shape}")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 2: CONVERSIÓN DE TIPOS FDC_NUTRIENT
# ====================================================================================

print("\n2. Conversión de Tipos en FDC_NUTRIENT")

try:
    df_nutrient = pd.read_sql("SELECT * FROM FDC_NUTRIENT_CLEAN", conn)
    print(f"  Dimensiones originales: {df_nutrient.shape}")
    
    # Conversiones de tipo
    df_nutrient['id'] = df_nutrient['id'].astype('int16')
    df_nutrient['name'] = df_nutrient['name'].astype(str).str.strip()
    df_nutrient['unit_name'] = df_nutrient['unit_name'].astype('category')
    
    # nutrient_nbr y rank de float a int
    if 'nutrient_nbr' in df_nutrient.columns:
        df_nutrient['nutrient_nbr'] = pd.to_numeric(df_nutrient['nutrient_nbr'], errors='coerce').fillna(0).astype('int16')
    
    if 'rank' in df_nutrient.columns:
        df_nutrient['rank'] = pd.to_numeric(df_nutrient['rank'], errors='coerce').fillna(0).astype('int16')
    
    df_nutrient.to_sql("FDC_NUTRIENT_TRANSFORM", conn, if_exists="replace", index=False)
    print(f"  ✓ FDC_NUTRIENT_TRANSFORM creada: {df_nutrient.shape}")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 3: TRANSFORMACIONES FDC_FOOD_NUTRIENT
# ====================================================================================

print("\n3. Transformaciones en FDC_FOOD_NUTRIENT (Tabla más importante)")

try:
    df_fn = pd.read_sql("SELECT * FROM FDC_FOOD_NUTRIENT_CLEAN", conn)
    print(f"  Dimensiones originales: {df_fn.shape}")
    print(f"  Nulos totales: {df_fn.isnull().sum().sum():,}")
    
    # Conversiones de tipo
    df_fn['id'] = df_fn['id'].astype('int32')
    df_fn['fdc_id'] = df_fn['fdc_id'].astype('int32')
    df_fn['nutrient_id'] = df_fn['nutrient_id'].astype('int16')
    df_fn['amount'] = df_fn['amount'].astype('float32')
    
    # data_points y derivation_id de float a int
    if 'data_points' in df_fn.columns:
        df_fn['data_points'] = pd.to_numeric(df_fn['data_points'], errors='coerce').fillna(0).astype('int8')
    
    if 'derivation_id' in df_fn.columns:
        df_fn['derivation_id'] = pd.to_numeric(df_fn['derivation_id'], errors='coerce').fillna(0).astype('int8')
    
    # Eliminar columnas con muchos nulos (>90%)
    umbral_nulos_fdc = 0.90
    porcentaje_nulos = df_fn.isnull().mean()
    cols_eliminar = porcentaje_nulos[porcentaje_nulos > umbral_nulos_fdc].index.tolist()
    
    if cols_eliminar:
        print(f"  Eliminando columnas con >90% nulos: {cols_eliminar}")
        df_fn = df_fn.drop(columns=cols_eliminar)
    
    df_fn.to_sql("FDC_FOOD_NUTRIENT_TRANSFORM", conn, if_exists="replace", index=False)
    print(f"  ✓ FDC_FOOD_NUTRIENT_TRANSFORM creada: {df_fn.shape}")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 4: CREAR TABLA MAESTRA FDC (PIVOT DE NUTRIENTES)
# ====================================================================================

print("\n4. Creando Tabla Maestra FDC con Nutrientes en Columnas")

try:
    # Esta es la transformación más importante: convertir nutrientes de filas a columnas
    print("  Cargando datos para pivot...")
    
    query_pivot = """
        SELECT 
            fn.fdc_id,
            n.name as nutrient_name,
            fn.amount
        FROM FDC_FOOD_NUTRIENT_TRANSFORM fn
        INNER JOIN FDC_NUTRIENT_TRANSFORM n ON fn.nutrient_id = n.id
        WHERE fn.amount IS NOT NULL
    """
    
    df_pivot_data = pd.read_sql(query_pivot, conn)
    print(f"  Datos cargados: {df_pivot_data.shape}")
    
    # Filtrar solo nutrientes principales para evitar tabla muy grande
    nutrientes_principales = [
        'Energy', 'Protein', 'Total lipid (fat)', 'Carbohydrate, by difference',
        'Fiber, total dietary', 'Sugars, total including NLEA', 'Calcium, Ca',
        'Iron, Fe', 'Magnesium, Mg', 'Phosphorus, P', 'Potassium, K', 
        'Sodium, Na', 'Zinc, Zn', 'Vitamin C, total ascorbic acid',
        'Thiamin', 'Riboflavin', 'Niacin', 'Vitamin B-6', 'Folate, total',
        'Vitamin B-12', 'Vitamin A, RAE', 'Vitamin E (alpha-tocopherol)',
        'Vitamin D (D2 + D3)', 'Vitamin K (phylloquinone)',
        'Fatty acids, total saturated', 'Fatty acids, total monounsaturated',
        'Fatty acids, total polyunsaturated', 'Cholesterol'
    ]
    
    df_pivot_filtered = df_pivot_data[df_pivot_data['nutrient_name'].isin(nutrientes_principales)]
    print(f"  Datos filtrados (nutrientes principales): {df_pivot_filtered.shape}")
    
    # Crear pivot table
    print("  Creando pivot table (esto puede tardar)...")
    df_pivot = df_pivot_filtered.pivot_table(
        index='fdc_id',
        columns='nutrient_name',
        values='amount',
        aggfunc='mean'  # En caso de duplicados, promediar
    ).reset_index()
    
    # Renombrar columnas para facilitar uso
    col_rename = {
        'Energy': 'KCAL',
        'Protein': 'PROTEIN_G',
        'Total lipid (fat)': 'FAT_G',
        'Carbohydrate, by difference': 'CARB_G',
        'Fiber, total dietary': 'FIBER_G',
        'Sugars, total including NLEA': 'SUGAR_G',
        'Calcium, Ca': 'CALCIUM_MG',
        'Iron, Fe': 'IRON_MG',
        'Sodium, Na': 'SODIUM_MG',
        'Potassium, K': 'POTASSIUM_MG',
        'Vitamin C, total ascorbic acid': 'VIT_C_MG',
        'Vitamin A, RAE': 'VIT_A_UG',
        'Cholesterol': 'CHOLESTEROL_MG',
        'Fatty acids, total saturated': 'SAT_FAT_G'
    }
    
    df_pivot = df_pivot.rename(columns=col_rename)
    
    # Unir con información del alimento
    df_food_info = pd.read_sql(
        "SELECT fdc_id, description, data_type, food_category_id FROM FDC_FOOD_TRANSFORM",
        conn
    )
    
    df_fdc_master = df_pivot.merge(df_food_info, on='fdc_id', how='left')
    
    print(f"  ✓ Pivot completado: {df_fdc_master.shape}")
    print(f"  Columnas de nutrientes: {len([c for c in df_fdc_master.columns if c not in ['fdc_id', 'description', 'data_type', 'food_category_id']])}")
    
    df_fdc_master.to_sql("FDC_MAESTRO", conn, if_exists="replace", index=False)
    print(f"  ✓ FDC_MAESTRO creada: {df_fdc_master.shape}")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 5: NORMALIZACIÓN DE NUTRIENTES
# ====================================================================================

print("\n5. Normalización de Valores Nutricionales")

try:
    df_master = pd.read_sql("SELECT * FROM FDC_MAESTRO", conn)
    
    # Identificar columnas numéricas (nutrientes)
    nutrient_cols = [col for col in df_master.columns 
                     if col not in ['fdc_id', 'description', 'data_type', 'food_category_id']]
    
    scaler = MinMaxScaler()  # Usar MinMax para mantener interpretabilidad (0-1)
    
    for col in nutrient_cols:
        if col in df_master.columns:
            mask = df_master[col].notna()
            if mask.sum() > 0:
                df_master.loc[mask, f'{col}_NORM'] = scaler.fit_transform(
                    df_master.loc[mask, [col]]
                )
    
    nuevas_cols = [c for c in df_master.columns if '_NORM' in c]
    print(f"  ✓ {len(nuevas_cols)} columnas normalizadas creadas")
    
    df_master.to_sql("FDC_MAESTRO_NORM", conn, if_exists="replace", index=False)
    print(f"  ✓ FDC_MAESTRO_NORM creada: {df_master.shape}")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# RESUMEN FINAL
# ====================================================================================

print("\n" + "=" * 80)
print("RESUMEN DE TRANSFORMACIONES FDC")
print("=" * 80)

cursor = conn.cursor()

print("\n📊 Tablas FDC Transformadas:")
tablas_fdc = [
    'FDC_FOOD_TRANSFORM',
    'FDC_NUTRIENT_TRANSFORM',
    'FDC_FOOD_NUTRIENT_TRANSFORM',
    'FDC_MAESTRO',
    'FDC_MAESTRO_NORM'
]

for tabla in tablas_fdc:
    try:
        count = cursor.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
        cols = cursor.execute(f"PRAGMA table_info({tabla})").fetchall()
        print(f"  ✓ {tabla}: {count:,} registros × {len(cols)} columnas")
    except Exception as e:
        print(f"  ⚠️  {tabla}: No disponible ({e})")

print("\n" + "=" * 80)
print("✅ TRANSFORMACIONES FDC COMPLETADAS")
print("=" * 80)
print("\n🔗 Conexión SQLite mantenida abierta para siguientes transformaciones")
print("=" * 80)

# Nota: No cerrar la conexión aquí si se necesita en siguientes scripts
# conn.close()
