import pandas as pd
import requests
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

URL = 'https://www.datos.gov.co/resource/p6dx-8zbt.json'
LIMIT = 1000        
MAX_REINTENTOS = 3  
PAUSA_REINTENTO = 5 
# aca empece aa usar logging en vez de print porque es mas especifico
def descargar_con_reintentos(url, params, max_reintentos=MAX_REINTENTOS):
    for intento in range(max_reintentos):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()  # Lanza excepcion si status != 200
            return response.json()
        except requests.exceptions.RequestException as e:
            log.warning(f'Intento {intento+1}/{max_reintentos} fallido: {e}')
            if intento < max_reintentos - 1:
                time.sleep(PAUSA_REINTENTO)
    log.error('Todos los reintentos fallaron.')
    return None

def descargar_rango(fecha_desde, fecha_hasta, archivo_salida):
    offset = 0
    data = []

    while True:
        params = {
            '$limit': LIMIT,   
            '$offset': offset,
            '$where': f"fecha_de_publicacion >= '{fecha_desde}' "
                      f"AND fecha_de_publicacion < '{fecha_hasta}'"
        }

        batch = descargar_con_reintentos(URL, params)

        if batch is None:
            log.error('Descarga fallida. Guardando lo que se tiene.')
            break

        if not batch:
            log.info('No hay mas datos. Descarga completa.')
            break

        data.extend(batch)
        log.info(f'Descargadas {len(data)} filas (offset={offset})...')

        offset += LIMIT

        time.sleep(0.5)

    if not data:
        log.warning('No se descargo ningun dato.')
        return

    df = pd.DataFrame(data)

    if 'fecha_de_publicacion' in df.columns:
        df['fecha_de_publicacion'] = pd.to_datetime(
            df['fecha_de_publicacion'], errors='coerce'
        )

    df.to_parquet(archivo_salida, index=False, engine='pyarrow')
    log.info(f'Guardado: {archivo_salida} ({len(df)} filas)')


if __name__ == '__main__':
    descargar_rango(
        fecha_desde='2026-02-16',
        fecha_hasta='2026-03-16',
        archivo_salida='secop.parquet'
    )
