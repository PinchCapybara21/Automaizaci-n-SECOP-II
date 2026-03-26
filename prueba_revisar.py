import pandas as pd
import requests
import time
import os
from datetime import timedelta

# ============== PERIMER PASO PARA LA PRUBEBA, REPETIR TODO LO ANTERIROR UN PERIDOD ANTES ====================
url = 'https://www.datos.gov.co/resource/p6dx-8zbt.json'
offset = 0
limit = 1000 #esto solo usar si necesitas mas de mil datos que es el maximo que devuelve la API, si no, puedes omitirlo
data = []

while True:
    params = {
        '$offset': offset,
        '$limit': limit, #si necesito mas de mil usarlo
        '$where': "fecha_de_publicacion >= '2025-12-26' AND fecha_de_publicacion < '2026-12-26'"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        print("Error:", response.status_code)
        print(response.text) 
        break

    batch = response.json()

    if not batch:
        break

    data.extend(batch)

    print(f"Descargadas {len(data)} filas...")

    offset += limit #si necesito mas de mil usarlo
   

df = pd.DataFrame(data)

df.to_parquet('secop_3m.parquet', index=False, engine='pyarrow')

print("Archivo guardado correctamente")

#===================== SEGUNDO PASO PARA LA PRUEBA, VER =========================
""""
archivo = 'secop_3m.parquet'
fecha_primer_descarga = '2026-01-16'
offset = 0
if os.path.exists(archivo):

    df_local = pd.read_parquet(archivo)
    
    df_local['fecha_de_publicacion'] = pd.to_datetime(df_local['fecha_de_publicacion'], errors='coerce')
    ultima_fecha = df_local['fecha_de_publicacion'].dt.date.max()
    fecha_desde = (ultima_fecha + timedelta(days=1)).strftime('%Y-%m-%d')
    print(f'Ultima fecha: {ultima_fecha}, fecha desde: {fecha_desde}')
else:
    fecha_desde = fecha_primer_descarga
    df_local = pd.DataFrame()
    print(f'No existe el archivo local, se descargará desde {fecha_desde}')

data_nuevos = []
while True:
    params = {
        '$offset': offset,
        '$limit': limit, #si necesito mas de mil usarlo
        '$where': f"fecha_de_publicacion >= '{fecha_desde}'"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        print("Error:", response.status_code)
        print(response.text) 
        break

    batch = response.json()

    if not batch:
        break

    data_nuevos.extend(batch)

    print(f"Descargadas {len(data_nuevos)} filas...")

    offset += limit #si necesito mas de mil usarlo

    
if not data_nuevos:
        print("No hay nuevos datos para descargar.")
else:   
    df_nuevos = pd.DataFrame(data_nuevos)
    df_nuevos['fecha_de_publicacion'] = pd.to_datetime(df_nuevos['fecha_de_publicacion'], errors='coerce')

    if not df_local.empty:
        df_completo = pd.concat([df_local, df_nuevos], ignore_index=True)
    else:
        df_completo = df_nuevos
    df_completo['fecha_de_publicacion'] = pd.to_datetime(df_completo['fecha_de_publicacion'], errors='coerce')
    df_completo = df_completo.dropna(subset=['fecha_de_publicacion'])
    df_completo = df_completo.drop_duplicates(subset=['id_del_proceso'])
    df_completo.to_parquet(archivo, index=False, engine='pyarrow')
    print(f"Archivo {archivo} actualizado correctamente con {len(df_nuevos)} nuevos registros.")

print("Proceso de revisión finalizado ✅")
"""