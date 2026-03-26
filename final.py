import pandas as pd
import requests
import logging
import shutil
import os
from datetime import timedelta
 
URL             = 'https://www.datos.gov.co/resource/p6dx-8zbt.json'
ARCHIVO         = 'secop.parquet'           # Tu parquet principal (el de descarga.py)
FECHA_INICIO    = '2026-02-16'              # Solo se usa si el archivo NO existe todavía
LIMIT           = 1000
 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
 
# ============================================================
# PASO 1: Leer el parquet existente y detectar desde qué fecha
#         hay que descargar datos nuevos
# ============================================================
if os.path.exists(ARCHIVO):
    log.info(f'Leyendo archivo existente: {ARCHIVO}')
    df_local = pd.read_parquet(ARCHIVO)
    df_local['fecha_de_publicacion'] = pd.to_datetime(
        df_local['fecha_de_publicacion'], errors='coerce'
    )
    ultima_fecha = df_local['fecha_de_publicacion'].dt.date.max()
    fecha_desde  = (ultima_fecha + timedelta(days=1)).strftime('%Y-%m-%d')
    log.info(f'Última fecha en el archivo: {ultima_fecha} → se descargará desde: {fecha_desde}')
else:
    log.warning(f'No existe {ARCHIVO}. Se descargará desde la fecha de inicio: {FECHA_INICIO}')
    fecha_desde = FECHA_INICIO
    df_local    = pd.DataFrame()
 
# ============================================================
# PASO 2: Descargar solo los registros nuevos (desde fecha_desde
#         hasta hoy, sin límite de fecha final)
# ============================================================
log.info(f'Iniciando descarga de registros desde {fecha_desde}...')
data_nuevos = []
offset = 0
 
while True:
    params = {
        '$offset': offset,
        '$limit' : LIMIT,
        '$where' : f"fecha_de_publicacion >= '{fecha_desde}'"
    }
 
    try:
        response = requests.get(URL, params=params, timeout=30)
    except requests.exceptions.RequestException as e:
        log.error(f'Error de conexión: {e}')
        raise
 
    if response.status_code != 200:
        log.error(f'Error HTTP {response.status_code}: {response.text}')
        raise Exception(f'Falló la descarga con código {response.status_code}')
 
    batch = response.json()
    if not batch:
        break
 
    data_nuevos.extend(batch)
    log.info(f'Descargadas {len(data_nuevos)} filas...')
    offset += LIMIT
 
# ============================================================
# PASO 3: Combinar, limpiar y guardar con escritura atómica
# ============================================================
if not data_nuevos:
    log.info('No hay nuevos datos para descargar. El archivo está al día ✅')
else:
    df_nuevos = pd.DataFrame(data_nuevos)
    df_nuevos['fecha_de_publicacion'] = pd.to_datetime(
        df_nuevos['fecha_de_publicacion'], errors='coerce'
    )
 
    # Combinar con los datos existentes
    if not df_local.empty:
        df_completo = pd.concat([df_local, df_nuevos], ignore_index=True)
    else:
        df_completo = df_nuevos
 
    # Limpiar fechas inválidas
    df_completo['fecha_de_publicacion'] = pd.to_datetime(
        df_completo['fecha_de_publicacion'], errors='coerce'
    )
    df_completo = df_completo.dropna(subset=['fecha_de_publicacion'])
 
    # Eliminar duplicados y reportar cuántos había
    antes = len(df_completo)
    df_completo = df_completo.drop_duplicates(subset=['id_del_proceso'])
    duplicados = antes - len(df_completo)
    if duplicados > 0:
        log.warning(f'Se eliminaron {duplicados} registros duplicados.')
 
    # Guardado atómico: escribe en .tmp y solo reemplaza si todo salió bien.
    # Así, si el proceso muere a mitad de escritura, el parquet original queda intacto.
    archivo_tmp = ARCHIVO + '.tmp'
    try:
        df_completo.to_parquet(archivo_tmp, index=False, engine='pyarrow')
        shutil.move(archivo_tmp, ARCHIVO)   # Operación atómica en el mismo disco
        log.info(f'Archivo actualizado: {len(df_nuevos)} nuevos registros. '
                 f'Total en disco: {len(df_completo)} registros.')
    except Exception as e:
        log.error(f'Error guardando archivo: {e}')
        if os.path.exists(archivo_tmp):
            os.remove(archivo_tmp)          # Limpia el temporal si falló
        raise
 
log.info('Proceso de actualización finalizado ✅')