import pandas as pd
import numpy as np
import sqlite3
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, LabelEncoder

# ====================================================================================
# CONFIGURACIÓN INICIAL Y CONEXIÓN A BASE DE DATOS
# ====================================================================================

print("=" * 80)
print("PIPELINE DE TRANSFORMACIONES NHANES")
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
# IDENTIFICACIÓN DE TABLAS NHANES
# ====================================================================================

print("\n" + "=" * 80)
print("IDENTIFICANDO TABLAS NHANES")
print("=" * 80)

cursor = conn.cursor()
tablas_existentes = [x[0] for x in cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table'"
).fetchall()]

# Filtrar tablas NHANES (las que contienen "_L_LIMPIO" o "LIMPIO")
tablas_nhanes = [t for t in tablas_existentes if "L_LIMPIO" in t or "LIMPIO" in t]

print(f"\nTablas NHANES encontradas ({len(tablas_nhanes)}):")
for tabla in tablas_nhanes:
    print(f"  - {tabla}")

# ====================================================================================
# PARTE 1: GESTIÓN DE VALORES FALTANTES
# ====================================================================================

print("\n" + "=" * 80)
print("1. GESTIÓN DE VALORES FALTANTES")
print("=" * 80)
print("Eliminando columnas con más del 70% de valores nulos\n")

umbral_nulos = 0.70

for tabla in tablas_nhanes:
    try:
        df = pd.read_sql(f"SELECT * FROM {tabla}", conn)
        print(f"📊 Procesando {tabla}: {df.shape}")

        # Eliminar columnas con >70% nulos
        porcentaje_nulos = df.isnull().mean()
        cols_eliminar = porcentaje_nulos[porcentaje_nulos > umbral_nulos].index.tolist()
        
        if cols_eliminar:
            print(f"   ⚠️  Columnas eliminadas ({len(cols_eliminar)}): {cols_eliminar[:5]}{'...' if len(cols_eliminar) > 5 else ''}")
            df = df.drop(columns=cols_eliminar)
        else:
            print(f"   ✓  No hay columnas para eliminar")

        # Guardar resultado con nombre _PROC
        nuevo_nombre = tabla.replace("_L_L_LIMPIO", "_PROC").replace("_LIMPIO", "_PROC")
        df.to_sql(nuevo_nombre, conn, if_exists="replace", index=False)
        print(f"   ✓  {nuevo_nombre}: {df.shape}\n")

    except Exception as e:
        print(f"   ✗  Error en {tabla}: {e}\n")

# ====================================================================================
# PARTE 2: CONVERSIÓN DE TIPOS DE DATOS
# ====================================================================================

print("=" * 80)
print("2. CONVERSIÓN DE TIPOS DE DATOS")
print("=" * 80)

try:
    df_demo = pd.read_sql("SELECT * FROM DEMO_L_PROC", conn)
    print(f"\n📊 Procesando DEMO_L_PROC: {df_demo.shape}")

    # Diccionario de conversiones de tipos
    conversiones = {
        'SEQN': 'int64',
        'SDDSRVYR': 'int8',
        'RIDSTATR': 'int8',
        'RIAGENDR': 'int8',
        'RIDAGEYR': 'int16',
        'RIDRETH1': 'int8',
        'RIDRETH3': 'int8',
        'RIDEXMON': 'int8',
        'DMDBORN4': 'int8',
        'DMDEDUC2': 'int8',
        'DMDMARTZ': 'int8',
        'DMDHHSIZ': 'int8',
        'DMDHRGND': 'int8',
        'DMDHRAGZ': 'int8',
        'SDMVPSU': 'int8'
    }

    columnas_convertidas = 0
    for col, dtype in conversiones.items():
        if col in df_demo.columns:
            df_demo[col] = pd.to_numeric(df_demo[col], errors='coerce').fillna(0).astype(dtype)
            columnas_convertidas += 1

    df_demo.to_sql("DEMO_L_TRANSFORM", conn, if_exists="replace", index=False)
    print(f"   ✓  {columnas_convertidas} columnas convertidas")
    print(f"   ✓  DEMO_L_TRANSFORM creada: {df_demo.shape}\n")

except Exception as e:
    print(f"   ✗  Error: {e}\n")

# ====================================================================================
# PARTE 3: NORMALIZACIÓN DE VARIABLES NUMÉRICAS
# ====================================================================================

print("=" * 80)
print("3. NORMALIZACIÓN DE VARIABLES NUMÉRICAS")
print("=" * 80)
print("Aplicando StandardScaler a variables numéricas\n")

scaler = StandardScaler()

# Detectar tablas con datos biomédicos para normalizar
palabras_clave = ["GLU", "HDL", "TRIGLY", "TCHOL", "INS"]
tablas_normalizar = [t for t in tablas_nhanes if any(palabra in t for palabra in palabras_clave)]

if not tablas_normalizar:
    print("⚠️  No se encontraron tablas para normalizar con las palabras clave especificadas")
else:
    for tabla in tablas_normalizar:
        try:
            df = pd.read_sql(f"SELECT * FROM {tabla}", conn)
            print(f"📊 Procesando {tabla}: {df.shape}")
            
            # Seleccionar solo columnas numéricas
            num_cols = df.select_dtypes(include='number').columns.tolist()

            if not num_cols:
                print(f"   ⚠️  No tiene columnas numéricas\n")
                continue

            # Crear copia y normalizar
            df_scaled = df.copy()
            df_scaled[num_cols] = scaler.fit_transform(df[num_cols].fillna(0))
            
            # Guardar con nombre _NORM
            nuevo_nombre = tabla.replace("_L_L_LIMPIO", "_NORM").replace("_LIMPIO", "_NORM")
            df_scaled.to_sql(nuevo_nombre, conn, if_exists="replace", index=False)
            print(f"   ✓  {nuevo_nombre} creada con {len(num_cols)} columnas normalizadas\n")

        except Exception as e:
            print(f"   ✗  Error normalizando {tabla}: {e}\n")

# ====================================================================================
# RESUMEN FINAL
# ====================================================================================

print("=" * 80)
print("✅ TRANSFORMACIONES NHANES COMPLETADAS EXITOSAMENTE")
print("=" * 80)

# Mostrar tablas creadas
cursor = conn.cursor()
tablas_finales = [x[0] for x in cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table'"
).fetchall()]

tablas_procesadas = [t for t in tablas_finales if "_PROC" in t or "_TRANSFORM" in t or "_NORM" in t]

print(f"\nTablas generadas ({len(tablas_procesadas)}):")
for tabla in sorted(tablas_procesadas):
    try:
        count = pd.read_sql(f"SELECT COUNT(*) as count FROM {tabla}", conn).iloc[0]['count']
        print(f"  ✓ {tabla}: {count} registros")
    except:
        print(f"  - {tabla}")

print("\n" + "=" * 80)
print("Pipeline finalizado. Base de datos lista para análisis.")
print("=" * 80)

# Nota: No cerrar la conexión aquí si se necesita en siguientes scripts
# conn.close()
