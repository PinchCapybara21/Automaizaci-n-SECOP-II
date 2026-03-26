import pandas as pd
import requests
import time

url = 'https://www.datos.gov.co/resource/p6dx-8zbt.json'

offset = 0
#limit = 1000 esto solo usar si necesitas mas de mil datos que es el maximo que devuelve la API, si no, puedes omitirlo
data = []

while True:
    params = {
        '$offset': offset,
        '$where': "fecha_de_publicacion >= '2026-02-16' AND fecha_de_publicacion < '2026-03-16'"
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

    #offset += limit si necesito mas de mil usarlo
   

df = pd.DataFrame(data)

df.to_parquet('secop.parquet', index=False, engine='pyarrow')

print("Archivo guardado correctamente ✅")