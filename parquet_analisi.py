import pandas as pd
import numpy as np

df = pd.read_parquet('data/secop_3m.parquet')

print(f'Filas: {len(df):,}')
print(f'Columnas: {len(df.columns)}')
print(f'Memoria aprox: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB')

print(df.dtypes)

#transpuesto para ver mejor
print(df.head(3).T)

#mirar nulos
nulos = df.isnull().sum()
pct_nulos = (nulos / len(df) * 100).round(1)

calidad = pd.DataFrame({
    'nulos': nulos,
    'pct_nulos': pct_nulos,
    'completitud': (100 - pct_nulos).round(1)
}).sort_values('pct_nulos', ascending=False)

print('Columnas con mas del 50% de datos faltantes:')
print(calidad[calidad['pct_nulos'] > 50])

# Alertas de columnas criticas
criticas = ['id_del_proceso', 'entidad', 'modalidad_de_contratacion',
            'precio_base', 'fecha_de_publicacion']
print('\nCalidad en columnas criticas:')
print(calidad.loc[criticas])

categoricas = [
    'modalidad_de_contratacion', 'tipo_de_contrato', 'estado_del_procedimiento',
    'departamento_entidad', 'fase', 'adjudicado', 'unidad_de_duracion'
]

for col in categoricas:
    if col in df.columns:
        conteo = df[col].value_counts(dropna=False)
        print(f'\n--- {col} ({len(conteo)} valores unicos) ---')
        print(conteo.head(10))

#aca buscamos variaciones de nombres
df['modalidad_norm'] = df['modalidad_de_contratacion'].str.strip().str.upper()
print('\nVariaciones de modalidad (despues de normalizar):')
print(df['modalidad_norm'].value_counts())
# Convertir a numerico (vienen como string de la API)
numericas = ['precio_base', 'valor_total_adjudicacion', 'duracion',
             'respuestas_al_procedimiento', 'proveedores_unicos_con']

for col in numericas:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

print(df[numericas].describe())

#ver si hay nulos o negativos
precio_raro = df[df['precio_base'] <= 0]['precio_base'].count()
print(f'\nContratos con precio_base <= 0: {precio_raro}')

df['ratio_precio'] = df['valor_total_adjudicacion'] / df['precio_base']
sobreprecios = df[df['ratio_precio'] > 2]
print(f'Contratos donde adjudicacion > 2x precio base: {len(sobreprecios)}')

bins = [0, 1e6, 10e6, 100e6, 1e9, float('inf')]
labels = ['< 1M', '1-10M', '10-100M', '100M-1B', '> 1B']
df['rango_valor'] = pd.cut(df['precio_base'], bins=bins, labels=labels)
print('\nDistribucion por rango de valor (COP):')
print(df['rango_valor'].value_counts().sort_index())

df['fecha_de_publicacion'] = pd.to_datetime(
    df['fecha_de_publicacion'], errors='coerce'
)

df['semana'] = df['fecha_de_publicacion'].dt.to_period('W')
por_semana = df.groupby('semana').agg(
    contratos=('id_del_proceso', 'count'),
    valor_total=('precio_base', 'sum')
).reset_index()
print(por_semana)

df['dia_semana'] = df['fecha_de_publicacion'].dt.day_name()
print('\nPublicaciones por dia de la semana:')
print(df['dia_semana'].value_counts())

top_proveedores = df.groupby('nombre_del_proveedor').agg(
    num_contratos=('id_del_proceso', 'count'),
    valor_total=('valor_total_adjudicacion', 'sum')
).sort_values('valor_total', ascending=False).head(10)
print(top_proveedores)

df_adj = df[df['valor_total_adjudicacion'] > 0].copy()
df_adj = df_adj.sort_values('valor_total_adjudicacion', ascending=False)
df_adj['valor_acumulado'] = df_adj['valor_total_adjudicacion'].cumsum()
total = df_adj['valor_total_adjudicacion'].sum()
df_adj['pct_acumulado'] = df_adj['valor_acumulado'] / total * 100

n_80pct = (df_adj['pct_acumulado'] <= 80).sum()
print(f'\n{n_80pct} contratos concentran el 80% del valor total contratado')

top_entidades = df.groupby('entidad').agg(
    num_contratos=('id_del_proceso', 'count'),
    valor_total=('precio_base', 'sum')
).sort_values('valor_total', ascending=False).head(10)
print('\nTop 10 entidades por valor:')
print(top_entidades)
print('\n===== INDICADORES DE ALERTA =====\n')

total = len(df)
directa = df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False).sum()
print(f'1. Contratacion directa: {directa/total*100:.1f}% ({directa} contratos)')

un_oferente = (df['respuestas_al_procedimiento'].astype(float) == 1).sum()
print(f'2. Procesos con un solo oferente: {un_oferente} ({un_oferente/total*100:.1f}%)')

desiertos = df['estado_del_procedimiento'].str.contains('Desierto', case=False, na=False).sum()
print(f'3. Procesos desiertos: {desiertos} ({desiertos/total*100:.1f}%)')

df['es_finde'] = df['fecha_de_publicacion'].dt.dayofweek >= 5
finde = df['es_finde'].sum()
print(f'4. Publicados en fin de semana: {finde} ({finde/total*100:.1f}%)')

precio_cero = (df['precio_base'].astype(float) == 0).sum()
print(f'5. Precio base en cero: {precio_cero} ({precio_cero/total*100:.1f}%)')