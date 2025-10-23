import pandas as pd
import sqlite3
import numpy as np

conn = sqlite3.connect("pipeline.db")

# ----------------------------------------------------------------------------
# Verificar tablas FDC disponibles
# ----------------------------------------------------------------------------

print("\n1. Verificación de Tablas FDC")

cursor = conn.cursor()
tablas_fdc = cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'FDC_%' ORDER BY name;"
).fetchall()

print("Tablas FDC encontradas:")
for tabla in tablas_fdc:
    count = cursor.execute(f"SELECT COUNT(*) FROM {tabla[0]}").fetchone()[0]
    print(f"  - {tabla[0]}: {count:,} registros")

# ----------------------------------------------------------------------------
# Limpieza FDC_FOOD
# ----------------------------------------------------------------------------

print("\n2. Limpieza FDC_FOOD")
print("-" * 80)

df_food = pd.read_sql("SELECT * FROM FDC_FOOD", conn)
print(f"Dimensiones originales: {df_food.shape[0]:,} filas x {df_food.shape[1]} columnas")

# Eliminar filas sin descripción
df_food_clean = df_food[df_food['description'].notna()].copy()

# Normalizar descripciones
df_food_clean['description'] = df_food_clean['description'].str.strip()
df_food_clean['description'] = df_food_clean['description'].str.upper()

# Eliminar duplicados exactos por fdc_id
df_food_clean = df_food_clean.drop_duplicates(subset=['fdc_id'], keep='first')

# Eliminar columnas completamente vacías
df_food_clean = df_food_clean.dropna(axis=1, how='all')

print(f"Dimensiones finales: {df_food_clean.shape[0]:,} filas x {df_food_clean.shape[1]} columnas")
print(f"Registros eliminados: {df_food.shape[0] - df_food_clean.shape[0]:,}")

df_food_clean.to_sql("FDC_FOOD_CLEAN", conn, if_exists="replace", index=False)

# ----------------------------------------------------------------------------
# Limpieza FDC_NUTRIENT
# ----------------------------------------------------------------------------

print("\n3. Limpieza FDC_NUTRIENT")
print("-" * 80)

df_nutrient = pd.read_sql("SELECT * FROM FDC_NUTRIENT", conn)
print(f"Dimensiones originales: {df_nutrient.shape[0]:,} filas x {df_nutrient.shape[1]} columnas")

# Eliminar filas sin nombre de nutriente
df_nutrient_clean = df_nutrient[df_nutrient['name'].notna()].copy()

# Normalizar nombres
df_nutrient_clean['name'] = df_nutrient_clean['name'].str.strip()

# Eliminar duplicados por id
df_nutrient_clean = df_nutrient_clean.drop_duplicates(subset=['id'], keep='first')

# Eliminar columnas vacías
df_nutrient_clean = df_nutrient_clean.dropna(axis=1, how='all')

print(f"Dimensiones finales: {df_nutrient_clean.shape[0]:,} filas x {df_nutrient_clean.shape[1]} columnas")

df_nutrient_clean.to_sql("FDC_NUTRIENT_CLEAN", conn, if_exists="replace", index=False)

# ----------------------------------------------------------------------------
# Limpieza FDC_FOOD_NUTRIENT
# ----------------------------------------------------------------------------

print("\n4. Limpieza FDC_FOOD_NUTRIENT")
print("-" * 80)

df_food_nutrient = pd.read_sql("SELECT * FROM FDC_FOOD_NUTRIENT", conn)
print(f"Dimensiones originales: {df_food_nutrient.shape[0]:,} filas x {df_food_nutrient.shape[1]} columnas")

# Eliminar registros sin amount
df_fn_clean = df_food_nutrient[df_food_nutrient['amount'].notna()].copy()

# Convertir amount a numérico
df_fn_clean['amount'] = pd.to_numeric(df_fn_clean['amount'], errors='coerce')

# Eliminar valores negativos
df_fn_clean = df_fn_clean[df_fn_clean['amount'] >= 0]

# Verificar que fdc_id y nutrient_id existan en tablas relacionadas
valid_fdc_ids = set(df_food_clean['fdc_id'])
valid_nutrient_ids = set(df_nutrient_clean['id'])

df_fn_clean = df_fn_clean[
    df_fn_clean['fdc_id'].isin(valid_fdc_ids) & 
    df_fn_clean['nutrient_id'].isin(valid_nutrient_ids)
]

# Eliminar duplicados (mismo alimento, mismo nutriente)
df_fn_clean = df_fn_clean.drop_duplicates(subset=['fdc_id', 'nutrient_id'], keep='first')

# Eliminar columnas vacías
df_fn_clean = df_fn_clean.dropna(axis=1, how='all')

print(f"Dimensiones finales: {df_fn_clean.shape[0]:,} filas x {df_fn_clean.shape[1]} columnas")
print(f"Registros eliminados: {df_food_nutrient.shape[0] - df_fn_clean.shape[0]:,}")

df_fn_clean.to_sql("FDC_FOOD_NUTRIENT_CLEAN", conn, if_exists="replace", index=False)

# ----------------------------------------------------------------------------
# Limpieza FDC_FOOD_CATEGORY
# ----------------------------------------------------------------------------

print("\n5. Limpieza FDC_FOOD_CATEGORY")
print("-" * 80)

df_category = pd.read_sql("SELECT * FROM FDC_FOOD_CATEGORY", conn)
print(f"Dimensiones originales: {df_category.shape[0]:,} filas x {df_category.shape[1]} columnas")

# Eliminar categorías sin descripción
df_category_clean = df_category[df_category['description'].notna()].copy()

# Normalizar descripciones
df_category_clean['description'] = df_category_clean['description'].str.strip()

# Eliminar duplicados
df_category_clean = df_category_clean.drop_duplicates(subset=['id'], keep='first')

print(f"Dimensiones finales: {df_category_clean.shape[0]:,} filas x {df_category_clean.shape[1]} columnas")

df_category_clean.to_sql("FDC_FOOD_CATEGORY_CLEAN", conn, if_exists="replace", index=False)

# ----------------------------------------------------------------------------
# Limpieza FDC_FOOD_PORTION
# ----------------------------------------------------------------------------

print("\n6. Limpieza FDC_FOOD_PORTION")
print("-" * 80)

df_portion = pd.read_sql("SELECT * FROM FDC_FOOD_PORTION", conn)
print(f"Dimensiones originales: {df_portion.shape[0]:,} filas x {df_portion.shape[1]} columnas")

# Convertir columnas numéricas
numeric_cols = ['amount', 'gram_weight']
for col in numeric_cols:
    if col in df_portion.columns:
        df_portion[col] = pd.to_numeric(df_portion[col], errors='coerce')

# Eliminar registros sin gram_weight (esencial para conversiones)
df_portion_clean = df_portion[df_portion['gram_weight'].notna()].copy()

# Eliminar valores negativos o cero
df_portion_clean = df_portion_clean[df_portion_clean['gram_weight'] > 0]

# Verificar fdc_id válidos
df_portion_clean = df_portion_clean[df_portion_clean['fdc_id'].isin(valid_fdc_ids)]

# Eliminar columnas vacías
df_portion_clean = df_portion_clean.dropna(axis=1, how='all')

print(f"Dimensiones finales: {df_portion_clean.shape[0]:,} filas x {df_portion_clean.shape[1]} columnas")
print(f"Registros eliminados: {df_portion.shape[0] - df_portion_clean.shape[0]:,}")

df_portion_clean.to_sql("FDC_FOOD_PORTION_CLEAN", conn, if_exists="replace", index=False)

# ----------------------------------------------------------------------------
# Resumen y verificación de integridad
# ----------------------------------------------------------------------------

print("\n7. Verificación de Integridad Referencial")

# Verificar relaciones
fdc_ids_food = set(df_food_clean['fdc_id'])
fdc_ids_nutrient = set(df_fn_clean['fdc_id'])
nutrient_ids_nutrient = set(df_nutrient_clean['id'])
nutrient_ids_fn = set(df_fn_clean['nutrient_id'])

print(f"IDs comunes FOOD <-> FOOD_NUTRIENT: {len(fdc_ids_food & fdc_ids_nutrient):,}")
print(f"IDs comunes NUTRIENT <-> FOOD_NUTRIENT: {len(nutrient_ids_nutrient & nutrient_ids_fn):,}")

# ----------------------------------------------------------------------------
# Resumen final
# ----------------------------------------------------------------------------


print("RESUMEN LIMPIEZA FDC")


tablas_clean = cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'FDC_%_CLEAN' ORDER BY name;"
).fetchall()

print("\nTablas limpias creadas:")
for tabla in tablas_clean:
    count = cursor.execute(f"SELECT COUNT(*) FROM {tabla[0]}").fetchone()[0]
    print(f"  - {tabla[0]}: {count:,} registros")

conn.close()
print("\nLimpieza FDC completada")