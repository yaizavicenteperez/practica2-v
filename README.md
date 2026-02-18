# practica2-v
Práctica 2 Visualización: Gramática de Gráficos. DataOps

## Descripción
Se implementa un pipeline de datos en Dagster que carga la distribución de rentas del ISTAC así como información de códigos de municipios e islas y niveles educativos por munnnicipio. Se realiza una breve transformación de estos datos y se generan 5 visualizaciones que relacionan educación, tipo de ingresos y ubicación geográfica.

## Estructura del proyecto
Programas:
- __init__.py: pipeline de assets de Dagster
- lab_renta.py: prototipado de los gráficos con los datos de la renta.
- lab_municipios.py: prototipado de los gráficos con los datos de la renta y los codigos de municipios e islas.
- lab_estudios.py: prototipado de los gráficos con los datos de la renta, los códigos de municipios e islas y el nivel de estudio.

Datos:
- distribucion-rentas-canarias.csv: datos de renta (ISTAC)
- codislas.csv: códigos de municipios e islas
- nivelestudios.xlsx: nivel educativo por municipio

## Visualizaciones generadas
1. Gráfico de barras apiladas: 'Estructura de ingresos por isla (2023)'
2. área chart facetado: 'Evolución temporal por isla (2015-2023)'
3. Box plot temporal: 'Variación de sueldos entre municipios'
4. Heatmap: 'Estructura de ingresos en Tenerife'
5. Scatter plot con correlación: 'Educación vs Desempleo'

## Paquetes utilizados
- Dagster
- Pandas
- Plotnine
- Openpyxl
