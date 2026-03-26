# Automaizaci-n-SECOP-II

################# DE ANALISIS 1 ###########################
min      2026-02-16 00:00:00
max      2026-03-24 00:00:00
1223 filas
53 columns
Memoria aprox: 4.6 MB

CICLO DE PROCESO
1. Publicado / Convocado	La entidad publico el proceso. Cualquier proveedor puede verlo y preparar su oferta.
2. Evaluacion / Seleccion	Se recibieron ofertas y la entidad esta revisandolas. Nadie mas puede ofertar.
3. Adjudicado	La entidad eligio un proveedor ganador. El contrato esta por firmarse.
4. En ejecucion	El contrato fue firmado y el proveedor esta ejecutandolo.
5. Liquidado / Terminado	El contrato termino. Se cierra el proceso.
6. Desierto	Nadie oferto, o las ofertas no cumplieron. Se puede volver a publicar.

TIPOS DE CONTRATACIÓN 
•	Licitacion Publica: La modalidad mas rigurosa, para contratos de alto valor. Cualquier empresa puede participar.
•	Seleccion Abreviada: Para montos intermedios. Proceso mas rapido que licitacion.
•	Concurso de Meritos: Para contratar servicios profesionales o consultoria. Se evalua calidad, no solo precio.
•	Contratacion Directa: Sin competencia. Solo en causales expresamente autorizadas por ley (urgencia manifiesta, monopolios, etc).
•	Minima Cuantia: Para contratos pequenos (hasta el 10% de la menor cuantia). Proceso muy simplificado.
•	Acuerdo Marco de Precios: La entidad compra a proveedores ya preseleccionados por Colombia Compra Eficiente

Calidad en columnas criticas:
                           nulos  pct_nulos  completitud
id_del_proceso                 0        0.0        100.0
entidad                        0        0.0        100.0
modalidad_de_contratacion      0        0.0        100.0
precio_base                    0        0.0        100.0
fecha_de_publicacion           0        0.0        100.0

--- modalidad_de_contratacion (1 valores unicos) ---
modalidad_de_contratacion
Selección Abreviada de Menor Cuantía    1223
Name: count, dtype: int64
En este df solo hay 1 modalidd de contratación

--- tipo_de_contrato (8 valores unicos) ---
tipo_de_contrato
Prestación de servicios     665
Obra                        270
Suministros                 132
Seguros                     104
Otro                         27
Compraventa                  15
Arrendamiento de muebles      9
Negocio fiduciario            1
Name: count, dtype: int64

--- estado_del_procedimiento (6 valores unicos) ---
estado_del_procedimiento
Evaluación       977
Publicado        224
Cancelado         13
Borrador           6
Aprobado           2
En aprobación      1
Name: count, dtype: int64

--- departamento_entidad (32 valores unicos) ---
departamento_entidad
Cundinamarca                  170
Distrito Capital de Bogotá    140
Antioquia                     113
Boyacá                        100
Valle del Cauca                96
Caldas                         62
Santander                      61
Bolívar                        44
Tolima                         40
No Definido                    36
Name: count, dtype: int64

---  Distribucion por rango de valor (COP):
rango_valor
< 1M         0
1-10M        0
10-100M    157
100M-1B    955
> 1B       111
Name: count, dtype: int64

Top 10 entidades por valor:
                                           num_contratos  valor_total
entidad
MUNICIPIO DE MANIZALES                                29  21304676522
ARMADA NACIONAL - DADIN                                3  14222502800
RASES - HUILA No.2                                     3  13535000000
HOSPITAL CENTRAL DE LA POLICIA                         2  13100000000
GOBERNACION DEL AMAZONAS                               1  12050086796
POLICIA METROPOLITANA DEL VALLE DE ABURRA              3  10276591965
AEROCIVIL                                             18  10108152333
ARMADA NACIONAL BASE NAVAL ARC MALAGA                  1   9573000000
REGIONAL DE ASEGURAMIENTO EN SALUD N°6                 6   9185000000
REGIONAL DE ASEGURAMIENTO EN SALUD No. 1               5   8618000000

===== INDICADORES DE ALERTA =====

1. Contratacion directa: 0.0% (0 contratos)
2. Procesos con un solo oferente: 0 (0.0%)
3. Procesos desiertos: 0 (0.0%)
4. Publicados en fin de semana: 10 (0.8%)
AEROCIVIL                                             18  10108152333
ARMADA NACIONAL BASE NAVAL ARC MALAGA                  1   9573000000
REGIONAL DE ASEGURAMIENTO EN SALUD N°6                 6   9185000000
REGIONAL DE ASEGURAMIENTO EN SALUD No. 1               5   8618000000

===== INDICADORES DE ALERTA =====

1. Contratacion directa: 0.0% (0 contratos)
2. Procesos con un solo oferente: 0 (0.0%)
3. Procesos desiertos: 0 (0.0%)
4. Publicados en fin de semana: 10 (0.8%)
5. Precio base en cero: 0 (0.0%)


Podemos evidenciar una alata contratación ~33 procesos/día en promedio
podemos ver que el 100% de la contratación de he hecho por medio de Selección Abreviada de Menor Cuantía la cual puede ser un buen indicio de 0 red flags

#Analisis 2.
Filas: 1,680
Columnas: 54
Memoria aprox: 6.5 MB

es lo mismo, no hay datos nulos, la modalida de contratacion es Selección Abreviada de Menor Cuantía, el tipo de contraato que mas hay es prestacion de servicios
===== RESUMEN MENSUAL =====
         num_contratos   valor_total  valor_promedio  pct_directa
mes
2025-12             26    6340173688    2.438528e+08          0.0
2026-01            201  103843530601    5.166345e+08          0.0
2026-02            652  291613061769    4.472593e+08          0.0
2026-03            801  414226381969    5.171366e+08          0.0


--- departamento_entidad (32 valores unicos) ---
departamento_entidad
Cundinamarca                  236
Distrito Capital de Bogotá    166
Boyacá                        162
Antioquia                     147
Valle del Cauca               135
Caldas                         92
Santander                      80
Bolívar                        57
Tolima                         55
Norte de Santander             52
Name: count, dtype: int64

Distribucion por rango de valor (COP):
rango_valor
< 1M          0
1-10M         1
10-100M     198
100M-1B    1326
> 1B        155
Name: count, dtype: int64

Top 10 entidades por valor:
                                                    num_contratos  valor_total
entidad
MUNICIPIO DE MANIZALES                                         37  27339975348
HOSPITAL CENTRAL DE LA POLICIA                                  3  21600000000
ARMADA NACIONAL - DADIN                                         3  14222502800
REGIONAL DE ASEGURAMIENTO EN SALUD N° 8                         5  14150925000
RASES - HUILA No.2                                              3  13535000000
GOBERNACION DEL AMAZONAS                                        1  12050086796
REGIONAL DE ASEGURAMIENTO EN SALUD N°6                          7  11644000000
ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE C...             10  11083389111
ARMADA NACIONAL BASE NAVAL ARC MALAGA                           2  10538000000
POLICIA METROPOLITANA DEL VALLE DE ABURRA                       3  10276591965

===== INDICADORES DE ALERTA =====
4. Publicados en fin de semana: 21 (1.2%)
