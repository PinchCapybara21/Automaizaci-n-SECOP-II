import pandas as pd
 
df = pd.read_parquet('data/secop_3m.parquet', engine='pyarrow')
 
# FIX: Convertir a datetime por si viene como string desde el parquet
df['fecha_de_publicacion'] = pd.to_datetime(df['fecha_de_publicacion'], errors='coerce')
 
# Eliminar filas sin fecha válida
df = df.dropna(subset=['fecha_de_publicacion'])
 
# Convertir precio_base a numérico por si viene como string
df['precio_base'] = pd.to_numeric(df['precio_base'], errors='coerce')
 
df['mes'] = df['fecha_de_publicacion'].dt.to_period('M')
 
resumen_mensual = df.groupby('mes').agg(
    num_contratos=('id_del_proceso', 'count'),
    valor_total=('precio_base', 'sum'),
    valor_promedio=('precio_base', 'mean'),
    pct_directa=('modalidad_de_contratacion',
                 lambda x: (x.str.contains('Directa', case=False, na=False).mean() * 100))
)
 
print('===== RESUMEN MENSUAL =====')
print(resumen_mensual.to_string())
 
# Variación mes a mes
resumen_mensual['var_contratos'] = resumen_mensual['num_contratos'].pct_change() * 100
resumen_mensual['var_valor']     = resumen_mensual['valor_total'].pct_change() * 100
 
print('\n===== VARIACIÓN PORCENTUAL MENSUAL =====')
print(resumen_mensual[['var_contratos', 'var_valor']].to_string())