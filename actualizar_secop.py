import pandas as pd
import requests
import os
from datetime import timedelta

# ===================== CONFIGURACIÓN =====================
URL = 'https://www.datos.gov.co/resource/p6dx-8zbt.json'
ARCHIVO = 'data/secop_3m.parquet'
LIMIT = 1000

# ===================== PASO 1: LEER ARCHIVO LOCAL =====================
print("=" * 55)
print("📂 REVISANDO ARCHIVO LOCAL")
print("=" * 55)

if os.path.exists(ARCHIVO):
    df_local = pd.read_parquet(ARCHIVO)
    df_local['fecha_de_publicacion'] = pd.to_datetime(df_local['fecha_de_publicacion'], errors='coerce')

    ultima_fecha = df_local['fecha_de_publicacion'].dt.date.max()
    primera_fecha = df_local['fecha_de_publicacion'].dt.date.min()
    fecha_desde = (ultima_fecha + timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"✅ Archivo encontrado: {ARCHIVO}")
    print(f"   Registros actuales : {len(df_local):,}")
    print(f"   Fecha más antigua  : {primera_fecha}")
    print(f"   Fecha más reciente : {ultima_fecha}")
    print(f"   Buscar datos desde : {fecha_desde}")
else:
    print(f"⚠️  No se encontró {ARCHIVO}. Se necesita una descarga inicial.")
    exit(1)

# ===================== PASO 2: BUSCAR DATOS NUEVOS EN LA API =====================
print()
print("=" * 55)
print("🌐 CONSULTANDO API - DATOS NUEVOS")
print("=" * 55)

offset = 0
data_nuevos = []

while True:
    params = {
        '$offset': offset,
        '$limit': LIMIT,
        '$where': f"fecha_de_publicacion >= '{fecha_desde}'"
    }

    response = requests.get(URL, params=params)

    if response.status_code != 200:
        print(f"❌ Error en la API: {response.status_code}")
        print(response.text)
        exit(1)

    batch = response.json()

    if not batch:
        break

    data_nuevos.extend(batch)
    print(f"   Descargando... {len(data_nuevos)} filas encontradas")
    offset += LIMIT

# ===================== PASO 3: RESULTADO DE LA BÚSQUEDA =====================
print()
print("=" * 55)
print("📊 RESULTADO")
print("=" * 55)

if not data_nuevos:
    print("✅ No hay datos nuevos. El archivo está al día.")
    print()
    print("No se realizaron cambios en el archivo local.")
    exit(0)

print(f"🆕 Se encontraron {len(data_nuevos):,} registros nuevos.")

df_nuevos = pd.DataFrame(data_nuevos)
df_nuevos['fecha_de_publicacion'] = pd.to_datetime(df_nuevos['fecha_de_publicacion'], errors='coerce')

# ===================== PASO 4: DIAGNÓSTICO SIN LIMPIAR =====================
print()
print("=" * 55)
print("🔍 DIAGNÓSTICO DE DATOS NUEVOS (sin modificar)")
print("=" * 55)

# Duplicados (solo avisamos, NO borramos)
if 'id_del_proceso' in df_nuevos.columns:
    duplicados = df_nuevos.duplicated(subset=['id_del_proceso']).sum()
    if duplicados > 0:
        print(f"⚠️  DUPLICADOS detectados: {duplicados:,} filas tienen id_del_proceso repetido.")
        print("   → No se eliminaron. Revisar manualmente si es necesario.")
    else:
        print("✅ Sin duplicados en los datos nuevos.")
else:
    print("⚠️  No se encontró la columna 'id_del_proceso' para revisar duplicados.")

# Nulos por columna (solo avisamos, NO borramos)
print()
print("📋 VALORES NULOS por columna en datos nuevos:")
nulos = df_nuevos.isnull().sum()
nulos_con_valores = nulos[nulos > 0]

if len(nulos_con_valores) == 0:
    print("   ✅ No hay valores nulos.")
else:
    for col, cantidad in nulos_con_valores.items():
        porcentaje = (cantidad / len(df_nuevos)) * 100
        print(f"   ⚠️  {col}: {cantidad:,} nulos ({porcentaje:.1f}%)")

# ===================== PASO 5: COMBINAR Y GUARDAR =====================
print()
print("=" * 55)
print("💾 GUARDANDO DATOS COMBINADOS")
print("=" * 55)

df_completo = pd.concat([df_local, df_nuevos], ignore_index=True)
df_completo['fecha_de_publicacion'] = pd.to_datetime(df_completo['fecha_de_publicacion'], errors='coerce')

print(f"   Registros antes    : {len(df_local):,}")
print(f"   Registros nuevos   : {len(df_nuevos):,}")
print(f"   Total combinado    : {len(df_completo):,}")

os.makedirs('data', exist_ok=True)
df_completo.to_parquet(ARCHIVO, index=False, engine='pyarrow')

print()
print(f"✅ Archivo guardado: {ARCHIVO}")
print("=" * 55)
print("Proceso finalizado correctamente ✅")
