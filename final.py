import pandas as pd
import requests
import logging
import shutil
import os
from datetime import timedelta
 
URL             = 'https://www.datos.gov.co/resource/p6dx-8zbt.json'
ARCHIVO         = 'secop.parquet'           
FECHA_INICIO    = '2026-02-16'
LIMIT           = 1000
 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
 
##### 1 ver los datos que hay y luego descargar solo los nuevos (desde la ultima fecha + 1 dia hasta hoy), 
##### falta añadir que busque en la nube, aun no se que nube es, preguntar 
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
 
# 2 descargar solo los nuevos datos desde la ultima fecha + 1 dia hasta hoy
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
 
# aca se medio limpia la tavuel pero no del todo
if not data_nuevos:
    log.info('No hay nuevos datos para descargar. El archivo está al día ✅')
else:
    df_nuevos = pd.DataFrame(data_nuevos)
    df_nuevos['fecha_de_publicacion'] = pd.to_datetime(
        df_nuevos['fecha_de_publicacion'], errors='coerce'
    )
 
    if not df_local.empty:
        df_completo = pd.concat([df_local, df_nuevos], ignore_index=True)
    else:
        df_completo = df_nuevos
 
    # aca se limpian fechas invalidas (preguntar porque creo que no se debe hacer, si se hace se pierden datos, pero si no se hace quedan datos basura)
    df_completo['fecha_de_publicacion'] = pd.to_datetime(
        df_completo['fecha_de_publicacion'], errors='coerce'
    )
    df_completo = df_completo.dropna(subset=['fecha_de_publicacion'])
 
    # Eliminar duplicados
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
        shutil.move(archivo_tmp, ARCHIVO)
        log.info(f'Archivo actualizado: {len(df_nuevos)} nuevos registros. '
                 f'Total en disco: {len(df_completo)} registros.')
    except Exception as e:
        log.error(f'Error guardando archivo: {e}')
        if os.path.exists(archivo_tmp):
            os.remove(archivo_tmp)
        raise
 
log.info('Proceso de actualización finalizado ✅')