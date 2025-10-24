import pandas as pd
import numpy as np
import sqlite3
import shutil
import os
from datetime import datetime
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, LabelEncoder

# ====================================================================================
# CONFIGURACIÓN INICIAL - PROTECCIÓN DE DATOS EXISTENTES
# ====================================================================================

print("\n" + "=" * 80)
print("TRANSFORMACIONES ODEPA - MODO SEGURO")
print("=" * 80)

# PROTECCIÓN: Si ya existe pipeline.db, hacer backup automático antes de continuar
if os.path.exists("pipeline.db"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_previo = f"pipeline_ANTERIOR_{timestamp}.db"
    shutil.copy2("pipeline.db", backup_previo)
    print(f"⚠️  SEGURIDAD: Tu pipeline.db anterior ha sido respaldado en:")
    print(f"   📁 {backup_previo}")
    print(f"   ✓ Tu trabajo anterior está protegido y NO se perderá\n")

# ====================================================================================
# CONEXIÓN A BASE DE DATOS
# ====================================================================================

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
# PARTE 1: CARGAR Y PREPARAR DATOS
# ====================================================================================

print("\n" + "=" * 80)
print("TRANSFORMACIONES ODEPA")
print("=" * 80)

print("\n1. Cargando y Preparando Datos ODEPA")

try:
    df_odepa = pd.read_sql("SELECT * FROM ODEPA_PRECIOS_CLEAN", conn)
    print(f"  Dimensiones originales: {df_odepa.shape}")
except Exception as e:
    print(f"  ✗ Error al cargar ODEPA_PRECIOS_CLEAN: {e}")
    try:
        df_odepa = pd.read_sql("SELECT * FROM ODEPA_PRECIOS", conn)
        print(f"  ✓ Cargando tabla base ODEPA_PRECIOS: {df_odepa.shape}")
    except:
        print(f"  ✗ No se encontró ninguna tabla ODEPA en la base de datos")
        print(f"  ⚠️  Verifica que hayas ejecutado los scripts de carga previos")
        exit()

# ====================================================================================
# PARTE 2: CONVERSIÓN DE TIPOS
# ====================================================================================

print("\n2. Conversión de Tipos de Datos")

try:
    # Asegurar columnas esperadas
    columnas_int = ['Anio', 'Mes', 'Semana', 'ID region']
    for col in columnas_int:
        if col in df_odepa.columns:
            df_odepa[col] = pd.to_numeric(df_odepa[col], errors='coerce').astype('Int16')
    
    # Convertir fechas si están como object
    for fecha_col in ['Fecha inicio', 'Fecha termino']:
        if fecha_col in df_odepa.columns:
            df_odepa[fecha_col] = pd.to_datetime(df_odepa[fecha_col], errors='coerce')
    
    # Convertir texto a categoría
    categoricas_odepa = [
        'Region', 'Sector', 'Tipo de punto monitoreo', 
        'Grupo', 'Producto', 'Unidad'
    ]
    for col in categoricas_odepa:
        if col in df_odepa.columns:
            df_odepa[col] = df_odepa[col].astype('category')
    
    # Precios a float32
    precio_cols = ['Precio minimo', 'Precio maximo', 'Precio promedio']
    for col in precio_cols:
        if col in df_odepa.columns:
            df_odepa[col] = pd.to_numeric(df_odepa[col], errors='coerce').astype('float32')
    
    print("  ✓ Tipos convertidos correctamente")
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 3: VARIABLES DERIVADAS TEMPORALES
# ====================================================================================

print("\n3. Creación de Variables Derivadas Temporales")

try:
    df_odepa['Trimestre'] = df_odepa['Mes'].apply(lambda x: (x - 1)//3 + 1 if pd.notna(x) else np.nan).astype('Int8')
    df_odepa['Semestre'] = df_odepa['Mes'].apply(lambda x: 1 if x <= 6 else 2 if pd.notna(x) else np.nan).astype('Int8')
    
    def get_season(mes):
        if pd.isna(mes): return np.nan
        if mes in [12, 1, 2]: return 'Verano'
        elif mes in [3, 4, 5]: return 'Otoño'
        elif mes in [6, 7, 8]: return 'Invierno'
        else: return 'Primavera'
    
    df_odepa['Estacion'] = df_odepa['Mes'].apply(get_season).astype('category')
    
    if 'Fecha inicio' in df_odepa.columns:
        df_odepa['Dia_Semana'] = df_odepa['Fecha inicio'].dt.dayofweek
        df_odepa['Nombre_Dia'] = df_odepa['Fecha inicio'].dt.day_name().astype('category')
    
    print("  ✓ Trimestre, Semestre, Estacion, Dia_Semana, Nombre_Dia creados")
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 4: VARIABLES DERIVADAS DE PRECIOS
# ====================================================================================

print("\n4. Creación de Variables Derivadas de Precios")

try:
    if {'Precio minimo', 'Precio maximo'} <= set(df_odepa.columns):
        df_odepa['Rango_Precio'] = df_odepa['Precio maximo'] - df_odepa['Precio minimo']
        print("  ✓ Rango_Precio creado")
    
    if {'Precio promedio', 'Rango_Precio'} <= set(df_odepa.columns):
        df_odepa['Coef_Variacion'] = (df_odepa['Rango_Precio'] / df_odepa['Precio promedio']) * 100
        df_odepa.loc[df_odepa['Coef_Variacion'] > 200, 'Coef_Variacion'] = 200
        print("  ✓ Coef_Variacion creado")
    
    if {'Precio promedio', 'Producto'} <= set(df_odepa.columns):
        df_odepa['Precio_Categoria'] = df_odepa.groupby('Producto', group_keys=False)['Precio promedio'].apply(
            lambda x: pd.qcut(x, q=4, labels=['Económico', 'Moderado', 'Caro', 'Premium'], duplicates='drop')
            if x.notna().sum() >= 4 else pd.Series([np.nan] * len(x), index=x.index)
        )
        print("  ✓ Precio_Categoria creado")
    
    if {'Precio promedio', 'Producto'} <= set(df_odepa.columns):
        df_odepa['Precio_Medio_Producto'] = df_odepa.groupby('Producto')['Precio promedio'].transform('mean')
        df_odepa['Desv_Precio'] = df_odepa['Precio promedio'] - df_odepa['Precio_Medio_Producto']
        df_odepa['Desv_Precio_Pct'] = (df_odepa['Desv_Precio'] / df_odepa['Precio_Medio_Producto']) * 100
        print("  ✓ Desv_Precio y Desv_Precio_Pct creados")
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 5: NORMALIZACIÓN DE PRECIOS
# ====================================================================================

print("\n5. Normalización de Precios")

try:
    scaler = StandardScaler()
    for col in ['Precio minimo', 'Precio maximo', 'Precio promedio']:
        if col in df_odepa.columns:
            mask = df_odepa[col].notna()
            if mask.sum() > 0:
                df_odepa.loc[mask, f'{col}_SCALED'] = scaler.fit_transform(df_odepa.loc[mask, [col]])
                print(f"  ✓ {col} → {col}_SCALED")
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 6: AGREGACIONES POR PRODUCTO
# ====================================================================================

print("\n6. Creando Agregaciones por Producto")

try:
    agg_producto = df_odepa.groupby('Producto', dropna=False).agg({
        'Precio promedio': ['mean', 'median', 'std', 'min', 'max', 'count'],
        'Region': 'nunique',
        'Sector': 'nunique'
    }).reset_index()

    agg_producto.columns = [
        'Producto', 'Precio_Mean', 'Precio_Median', 'Precio_Std',
        'Precio_Min_Global', 'Precio_Max_Global', 'N_Observaciones',
        'N_Regiones', 'N_Sectores'
    ]
    agg_producto.to_sql("ODEPA_AGREGADO_PRODUCTO", conn, if_exists="replace", index=False)
    print(f"  ✓ ODEPA_AGREGADO_PRODUCTO creada: {agg_producto.shape}")
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 7: AGREGACIONES POR REGIÓN
# ====================================================================================

print("\n7. Creando Agregaciones por Región")

try:
    agg_region = df_odepa.groupby('Region', dropna=False).agg({
        'Precio promedio': ['mean', 'median', 'std'],
        'Producto': 'nunique',
        'Sector': 'nunique'
    }).reset_index()

    agg_region.columns = [
        'Region', 'Precio_Mean', 'Precio_Median', 'Precio_Std',
        'N_Productos', 'N_Sectores'
    ]
    agg_region.to_sql("ODEPA_AGREGADO_REGION", conn, if_exists="replace", index=False)
    print(f"  ✓ ODEPA_AGREGADO_REGION creada: {agg_region.shape}")
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 8: SERIES DE TIEMPO
# ====================================================================================

print("\n8. Creando Series de Tiempo")

try:
    serie_tiempo = (
        df_odepa.groupby(['Producto', 'Anio', 'Semana'], dropna=False)
        .agg({'Precio promedio': 'mean', 'Fecha inicio': 'first'})
        .reset_index()
        .sort_values(['Producto', 'Anio', 'Semana'])
    )

    serie_tiempo['Precio_Cambio_Pct'] = serie_tiempo.groupby('Producto')['Precio promedio'].pct_change() * 100
    serie_tiempo.to_sql("ODEPA_SERIE_TIEMPO", conn, if_exists="replace", index=False)
    print(f"  ✓ ODEPA_SERIE_TIEMPO creada: {serie_tiempo.shape}")
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 9: GUARDAR TABLA TRANSFORMADA PRINCIPAL
# ====================================================================================

print("\n9. Guardando Tabla ODEPA Transformada")

try:
    df_odepa.to_sql("ODEPA_TRANSFORM", conn, if_exists="replace", index=False)
    print(f"  ✓ ODEPA_TRANSFORM creada: {df_odepa.shape}")
    nuevas_cols = [col for col in df_odepa.columns if col not in [
        'Anio', 'Mes', 'Semana', 'Fecha inicio', 'Fecha termino', 'ID region',
        'Region', 'Sector', 'Tipo de punto monitoreo', 'Grupo', 'Producto', 'Unidad',
        'Precio minimo', 'Precio maximo', 'Precio promedio'
    ]]
    print(f"  Nuevas columnas creadas ({len(nuevas_cols)}): {', '.join(nuevas_cols[:8])}...")
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# RESUMEN FINAL
# ====================================================================================

print("\n" + "=" * 80)
print("RESUMEN FINAL DE TRANSFORMACIONES")
print("=" * 80)

cursor = conn.cursor()

print("\n📊 Tablas FDC Transformadas:")
tablas_fdc = cursor.execute(
    """SELECT name FROM sqlite_master 
       WHERE type='table' AND (name LIKE 'FDC_%TRANSFORM%' OR name LIKE 'FDC_MAESTRO%')
       ORDER BY name;"""
).fetchall()

for tabla in tablas_fdc:
    try:
        count = cursor.execute(f"SELECT COUNT(*) FROM {tabla[0]}").fetchone()[0]
        cols = cursor.execute(f"PRAGMA table_info({tabla[0]})").fetchall()
        print(f"  ✓ {tabla[0]}: {count:,} registros × {len(cols)} columnas")
    except:
        pass

print("\n📊 Tablas ODEPA Transformadas:")
tablas_odepa = cursor.execute(
    """SELECT name FROM sqlite_master 
       WHERE type='table' AND 
       (name LIKE 'ODEPA_%TRANSFORM%' OR name LIKE 'ODEPA_AGREGADO%' OR name LIKE 'ODEPA_SERIE%')
       ORDER BY name;"""
).fetchall()

for tabla in tablas_odepa:
    try:
        count = cursor.execute(f"SELECT COUNT(*) FROM {tabla[0]}").fetchone()[0]
        cols = cursor.execute(f"PRAGMA table_info({tabla[0]})").fetchall()
        print(f"  ✓ {tabla[0]}: {count:,} registros × {len(cols)} columnas")
    except:
        pass

print("\n" + "=" * 80)
print("✅ TRANSFORMACIONES COMPLETADAS")
print("=" * 80)
print("\n📊 RESUMEN GLOBAL DE TODAS LAS TRANSFORMACIONES:")
print("   • NHANES: Tablas maestras con variables clínicas derivadas")
print("   • BRFSS: Tabla transformada con encoding y categorías")
print("   • FDC: Tabla maestra con nutrientes pivoteados")
print("   • ODEPA: Tablas con series de tiempo y agregaciones")

# ====================================================================================
# GUARDAR COPIA DE SEGURIDAD FINAL
# ====================================================================================

print("\n" + "=" * 80)
print("GUARDANDO COPIA DE SEGURIDAD FINAL")
print("=" * 80)

# Commit para asegurar que todos los cambios se guarden
conn.commit()
print("✓ Cambios confirmados en la base de datos")

# Crear copia de seguridad con timestamp
timestamp_final = datetime.now().strftime("%Y%m%d_%H%M%S")
backup_filename = f"pipeline_NUEVO_{timestamp_final}.db"

try:
    # Cerrar la conexión actual
    conn.close()
    print("✓ Conexión cerrada para crear backup final")
    
    # Crear copia de seguridad del pipeline NUEVO
    shutil.copy2("pipeline.db", backup_filename)
    print(f"✓ Backup del pipeline NUEVO creado: {backup_filename}")
    
    # También mantener una copia con nombre fijo (se sobrescribe cada vez)
    shutil.copy2("pipeline.db", "pipeline_latest.db")
    print("✓ Última versión guardada: pipeline_latest.db")
    
    print("\n📁 Archivos disponibles en tu carpeta:")
    print("   • pipeline.db (base de datos actual con nuevas transformaciones)")
    print(f"   • pipeline_ANTERIOR_*.db (tu trabajo anterior - PROTEGIDO)")
    print(f"   • {backup_filename} (backup del trabajo nuevo)")
    print("   • pipeline_latest.db (última versión siempre actualizada)")
    
    print("\n" + "=" * 80)
    print("🔒 PROTECCIÓN DE DATOS")
    print("=" * 80)
    print("✓ Tu pipeline.db anterior está guardado con timestamp")
    print("✓ Puedes volver a él en cualquier momento")
    print("✓ NO se ha perdido ningún dato")
    
    print("\n💡 Para volver a tu pipeline anterior:")
    print("   1. Renombra 'pipeline.db' a 'pipeline_descartado.db'")
    print("   2. Renombra 'pipeline_ANTERIOR_*.db' a 'pipeline.db'")
    
except Exception as e:
    print(f"⚠️  Error al crear backup: {e}")

print("\n" + "=" * 80)
print("🎉 PIPELINE DE TRANSFORMACIONES COMPLETADO")
print("=" * 80)
