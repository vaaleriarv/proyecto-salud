import pandas as pd
import sqlite3
import numpy as np

# Conectar a la base de datos SQLite
conn = sqlite3.connect("pipeline.db")
cursor = conn.cursor()

# -------------------------
# FUNCIONES DE LIMPIEZA
# -------------------------

def limpiar_fdc_tabla(tabla, columnas_id=['fdc_id'], columnas_texto=None):
    """
    Limpieza básica de tablas FDC: eliminar duplicados, columnas vacías, normalizar texto
    """
    df = pd.read_sql(f"SELECT * FROM {tabla}", conn)
    
    # Normalizar columnas de texto
    if columnas_texto:
        for col in columnas_texto:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()
    
    # Eliminar duplicados por id
    df = df.drop_duplicates(subset=columnas_id, keep='first')
    
    # Eliminar columnas completamente vacías
    df = df.dropna(axis=1, how='all')
    
    return df

def limpiar_odepa(df):
    """
    Limpieza ODEPA: precios, fechas, unidades, outliers, categorías de precio
    """
    # --- Precios ---
    for col in ['Precio minimo', 'Precio maximo', 'Precio promedio']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
    
    # --- Fechas ---
    for col in ['Fecha inicio', 'Fecha termino']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d')
    
    # --- Variables derivadas ---
    if 'Fecha inicio' in df.columns:
        df['año_fecha'] = df['Fecha inicio'].dt.year
        df['mes_fecha'] = df['Fecha inicio'].dt.month
        df['semana_año'] = df['Fecha inicio'].dt.isocalendar().week
    
    # --- Registros inválidos ---
    df = df[df['Producto'].notna() & (df['Producto'] != '')]
    df = df[df['Precio promedio'].notna() & (df['Precio promedio'] > 0)]
    df = df[df['Grupo'].notna() & (df['Grupo'] != '')]
    
    # --- Precios fuera de rango ---
    mask = (df['Precio promedio'] < df['Precio minimo']) | (df['Precio promedio'] > df['Precio maximo'])
    df.loc[mask, 'Precio promedio'] = (df.loc[mask, 'Precio minimo'] + df.loc[mask, 'Precio maximo']) / 2
    
    # --- Outliers ---
    Q1 = df['Precio promedio'].quantile(0.25)
    Q3 = df['Precio promedio'].quantile(0.75)
    IQR = Q3 - Q1
    lower, upper = Q1 - 3*IQR, Q3 + 3*IQR
    df['es_outlier'] = ((df['Precio promedio'] < lower) | (df['Precio promedio'] > upper)).astype(int)
    
    # --- Unidades normalizadas ---
    unidad_map = {
        '$/kilo': 'CLP/kg', '$/kilogramo': 'CLP/kg', '$/kg': 'CLP/kg',
        '$/litro': 'CLP/L', '$/lt': 'CLP/L', '$/unidad': 'CLP/unidad',
        '$/un': 'CLP/unidad', '$/docena': 'CLP/docena'
    }
    if 'Unidad' in df.columns:
        df['Unidad_normalizada'] = df['Unidad'].replace(unidad_map)
    
    # --- Eliminar columnas vacías o redundantes ---
    df = df.dropna(axis=1, how='all')
    columnas_redundantes = [c for c in df.columns if df[c].nunique() == 1 and c not in ['Unidad', 'Unidad_normalizada']]
    df = df.drop(columns=columnas_redundantes)
    
    return df

# -------------------------
# LIMPIEZA FDC
# -------------------------
print("\n--- LIMPIEZA FDC ---")

# FDC_FOOD
df_food_clean = limpiar_fdc_tabla('FDC_FOOD', columnas_id=['fdc_id'], columnas_texto=['description'])
df_food_clean.to_sql("FDC_FOOD_CLEAN", conn, if_exists="replace", index=False)

# FDC_NUTRIENT
df_nutrient_clean = limpiar_fdc_tabla('FDC_NUTRIENT', columnas_id=['id'], columnas_texto=['name'])
df_nutrient_clean.to_sql("FDC_NUTRIENT_CLEAN", conn, if_exists="replace", index=False)

# FDC_FOOD_NUTRIENT
df_fn = pd.read_sql("SELECT * FROM FDC_FOOD_NUTRIENT", conn)
df_fn_clean = df_fn[df_fn['amount'].notna()].copy()
df_fn_clean['amount'] = pd.to_numeric(df_fn_clean['amount'], errors='coerce')
df_fn_clean = df_fn_clean[df_fn_clean['amount'] >= 0]
# Mantener solo IDs válidos
valid_fdc_ids = set(df_food_clean['fdc_id'])
valid_nutrient_ids = set(df_nutrient_clean['id'])
df_fn_clean = df_fn_clean[df_fn_clean['fdc_id'].isin(valid_fdc_ids) & df_fn_clean['nutrient_id'].isin(valid_nutrient_ids)]
df_fn_clean = df_fn_clean.drop_duplicates(subset=['fdc_id','nutrient_id'], keep='first')
df_fn_clean.to_sql("FDC_FOOD_NUTRIENT_CLEAN", conn, if_exists="replace", index=False)

# -------------------------
# LIMPIEZA ODEPA
# -------------------------
print("\n--- LIMPIEZA ODEPA ---")
df_odepa = pd.read_sql("SELECT * FROM ODEPA_PRECIOS", conn)
df_odepa_clean = limpiar_odepa(df_odepa)
df_odepa_clean.to_sql("ODEPA_PRECIOS_CLEAN", conn, if_exists="replace", index=False)

# Crear tabla reciente (último año)
if 'año_fecha' in df_odepa_clean.columns:
    año_max = df_odepa_clean['año_fecha'].max()
    df_odepa_clean[df_odepa_clean['año_fecha'] == año_max].to_sql("ODEPA_PRECIOS_RECIENTES", conn, if_exists="replace", index=False)

# -------------------------
# RESUMEN
# -------------------------
print("\n--- RESUMEN ---")
tablas_clean = cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_CLEAN'").fetchall()
for t in tablas_clean:
    count = cursor.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
    print(f"{t[0]}: {count:,} registros")

conn.close()
print("\nIngesta y limpieza completadas ✅")
