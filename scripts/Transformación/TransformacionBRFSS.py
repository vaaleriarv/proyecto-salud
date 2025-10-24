import pandas as pd
import numpy as np
import sqlite3
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, LabelEncoder

# ====================================================================================
# CONFIGURACIÓN INICIAL Y CONEXIÓN A BASE DE DATOS
# ====================================================================================

print("\n" + "=" * 80)
print("TRANSFORMACIONES BRFSS")
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
# PARTE 1: CARGAR Y CONVERSIÓN DE TIPOS
# ====================================================================================

print("\n1. Conversión de Tipos de Datos")

try:
    df_brfss = pd.read_sql("SELECT * FROM BRFSS_2024_LIMPIO", conn)
    print(f"  Dimensiones originales: {df_brfss.shape}")
    
    # Conversiones a categorías (para variables ya mapeadas a texto)
    categoricas = [
        '_STATE', 'MARITAL', '_CHLDCNT', '_INCOMG1', '_AGE_G', 
        '_SEX', '_EDUCAG', '_URBSTAT', '_METSTAT', 'MEDCOST1', 'CHECKUP1'
    ]
    
    for col in categoricas:
        if col in df_brfss.columns:
            df_brfss[col] = df_brfss[col].astype('category')
    
    # Conversiones a numéricas (float32 para ahorrar memoria)
    numericas = [
        'WEIGHT2', 'WTKG3', 'HEIGHT3', '_BMI5', 
        'ALCDAY4', 'AVEDRNK4', 'DRNK3GE5', '_DRNKWK3'
    ]
    
    for col in numericas:
        if col in df_brfss.columns:
            df_brfss[col] = pd.to_numeric(df_brfss[col], errors='coerce').astype('float32')
    
    print(f"  ✓ Tipos convertidos correctamente")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 2: LIMPIEZA DE CÓDIGOS ESPECIALES
# ====================================================================================

print("\n2. Limpieza de Códigos Especiales (777, 888, 999)")

try:
    # Códigos que representan respuestas no válidas
    codigos_especiales = {
        777: np.nan,  # No sabe/Incertidumbre
        888: np.nan,  # Ninguno/No aplica
        999: np.nan,  # Se negó a responder
        5.397605e-79: np.nan  # Valor extraño en los datos
    }
    
    columnas_numericas = df_brfss.select_dtypes(include=['float32', 'float64']).columns
    
    for col in columnas_numericas:
        valores_antes = df_brfss[col].notna().sum()
        df_brfss[col] = df_brfss[col].replace(codigos_especiales)
        valores_despues = df_brfss[col].notna().sum()
        
        if valores_antes != valores_despues:
            print(f"  {col}: {valores_antes - valores_despues} códigos especiales reemplazados")
    
    print(f"  ✓ Códigos especiales limpiados")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 3: NORMALIZACIÓN DE VARIABLES NUMÉRICAS
# ====================================================================================

print("\n3. Normalización de Variables Numéricas (RobustScaler)")

try:
    # Usar RobustScaler por la presencia de outliers en BRFSS
    scaler = RobustScaler()
    
    vars_normalizar_brfss = ['WEIGHT2', 'WTKG3', 'HEIGHT3', '_BMI5', '_DRNKWK3']
    
    for col in vars_normalizar_brfss:
        if col in df_brfss.columns:
            mask = df_brfss[col].notna()
            if mask.sum() > 0:
                df_brfss.loc[mask, f'{col}_SCALED'] = scaler.fit_transform(
                    df_brfss.loc[mask, [col]]
                )
                print(f"  ✓ {col} → {col}_SCALED")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 4: CREACIÓN DE VARIABLES DERIVADAS
# ====================================================================================

print("\n4. Creación de Variables Derivadas")

try:
    # IMC categorizado (convertir formato XXYY a XX.YY)
    if '_BMI5' in df_brfss.columns:
        df_brfss['BMI_CALC'] = df_brfss['_BMI5'] / 100
        df_brfss['BMI_CATEGORY'] = pd.cut(
            df_brfss['BMI_CALC'],
            bins=[0, 18.5, 25, 30, 100],
            labels=['Bajo peso', 'Normal', 'Sobrepeso', 'Obesidad']
        )
        print("  ✓ BMI_CALC y BMI_CATEGORY creados")
    
    # Peso en libras a kg (si existe WEIGHT2)
    if 'WEIGHT2' in df_brfss.columns:
        # WEIGHT2 está en formato especial, necesita conversión
        df_brfss['WEIGHT_KG'] = df_brfss['WEIGHT2'] * 0.453592
        print("  ✓ WEIGHT_KG creado")
    
    # Altura en pulgadas a cm (si existe HEIGHT3)
    if 'HEIGHT3' in df_brfss.columns:
        df_brfss['HEIGHT_CM'] = df_brfss['HEIGHT3'] * 2.54
        print("  ✓ HEIGHT_CM creado")
    
    # Riesgo de consumo de alcohol
    if 'AVEDRNK4' in df_brfss.columns and 'DRNK3GE5' in df_brfss.columns:
        df_brfss['ALCOHOL_RISK'] = 'Bajo'
        df_brfss.loc[df_brfss['AVEDRNK4'] > 2, 'ALCOHOL_RISK'] = 'Moderado'
        df_brfss.loc[df_brfss['DRNK3GE5'] >= 2, 'ALCOHOL_RISK'] = 'Alto'
        df_brfss['ALCOHOL_RISK'] = df_brfss['ALCOHOL_RISK'].astype('category')
        print("  ✓ ALCOHOL_RISK creado")
    
    # Actividad física binaria
    if '_TOTINDA' in df_brfss.columns:
        df_brfss['PHYSICALLY_ACTIVE'] = (df_brfss['_TOTINDA'] == 1).astype('int8')
        print("  ✓ PHYSICALLY_ACTIVE creado")
    
    # Fumador actual (basado en SMOKDAY2)
    if 'SMOKDAY2' in df_brfss.columns:
        df_brfss['CURRENT_SMOKER'] = df_brfss['SMOKDAY2'].isin([1, 2]).astype('int8')
        print("  ✓ CURRENT_SMOKER creado")
    
    # Categoría de acceso a salud
    if 'MEDCOST1' in df_brfss.columns and '_HLTHPL2' in df_brfss.columns:
        df_brfss['HEALTH_ACCESS'] = 'Completo'
        df_brfss.loc[df_brfss['MEDCOST1'] == 'Sí, no pudo pagar', 'HEALTH_ACCESS'] = 'Limitado'
        df_brfss.loc[df_brfss['_HLTHPL2'] == 2, 'HEALTH_ACCESS'] = 'Sin seguro'
        df_brfss['HEALTH_ACCESS'] = df_brfss['HEALTH_ACCESS'].astype('category')
        print("  ✓ HEALTH_ACCESS creado")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 5: LABEL ENCODING PARA MODELOS
# ====================================================================================

print("\n5. Label Encoding para Variables Categóricas")

try:
    le = LabelEncoder()
    
    categorical_cols = ['_AGE_G', '_SEX', '_EDUCAG', 'MARITAL', '_URBSTAT', '_METSTAT']
    
    for col in categorical_cols:
        if col in df_brfss.columns:
            mask = df_brfss[col].notna()
            if mask.sum() > 0:
                df_brfss.loc[mask, f'{col}_ENCODED'] = le.fit_transform(
                    df_brfss.loc[mask, col].astype(str)
                )
                print(f"  ✓ {col} → {col}_ENCODED")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# PARTE 6: GUARDAR TABLA TRANSFORMADA
# ====================================================================================

print("\n6. Guardando Tabla BRFSS Transformada")

try:
    df_brfss.to_sql("BRFSS_2024_TRANSFORM", conn, if_exists="replace", index=False)
    print(f"  ✓ BRFSS_2024_TRANSFORM creada: {df_brfss.shape}")
    print(f"  Columnas totales: {len(df_brfss.columns)}")
    
    # Resumen de transformaciones
    nuevas_cols = [col for col in df_brfss.columns if '_SCALED' in col or '_ENCODED' in col or 
                   col in ['BMI_CALC', 'BMI_CATEGORY', 'WEIGHT_KG', 'HEIGHT_CM', 
                          'ALCOHOL_RISK', 'PHYSICALLY_ACTIVE', 'CURRENT_SMOKER', 'HEALTH_ACCESS']]
    print(f"  Nuevas columnas creadas ({len(nuevas_cols)}): {', '.join(nuevas_cols[:5])}...")
    
except Exception as e:
    print(f"  ✗ Error: {e}")

# ====================================================================================
# RESUMEN FINAL
# ====================================================================================

print("\n" + "=" * 80)
print("RESUMEN DE TRANSFORMACIONES")
print("=" * 80)

cursor = conn.cursor()

print("\n📊 Tablas Transformadas Creadas:")
tablas_transformadas = cursor.execute(
    """SELECT name FROM sqlite_master 
       WHERE type='table' AND (name LIKE '%TRANSFORM%' OR name LIKE '%MAESTRO%' 
       OR name LIKE '%FEATURES%' OR name LIKE '%PROC%' OR name LIKE '%NORM%') 
       ORDER BY name;"""
).fetchall()

for tabla in tablas_transformadas:
    try:
        count = cursor.execute(f"SELECT COUNT(*) FROM {tabla[0]}").fetchone()[0]
        cols = cursor.execute(f"PRAGMA table_info({tabla[0]})").fetchall()
        print(f"  ✓ {tabla[0]}: {count:,} registros × {len(cols)} columnas")
    except:
        pass

print("\n" + "=" * 80)
print("✅ TRANSFORMACIONES BRFSS COMPLETADAS")
print("=" * 80)
print("\n🔗 Conexión SQLite mantenida abierta para siguientes transformaciones")
print("=" * 80)

# Nota: No cerrar la conexión aquí si se necesita en siguientes scripts
# conn.close()
