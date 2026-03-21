
# Práctica 4 Visualización: Automatización - Generación de Código con IA

### Descripción
Se trabaja sobre el trabajo anterior, que encontramos en la rama `automatizacion` de este proyecto. Aquí comentaremos únicamente los archivos nuevos y los cambios que se han realizado. Se ha mantenido la misma funcionalidad de la Práctica 3 (assets, checks y definitions) pero se ha transformado el pipeline de visualización para que el código de los gráficos se genere automáticamente mediante un modelo de lenguaje (LLM) alojado en el servidor de la ULL.

### Estructura del proyecto (archivos nuevos o modificados)

**Programas:**
- `assets_file.py`: pipeline de assets de Dagster reorganizado en cinco etapas: carga, transformación, plantillas IA, generación de código y visualización. Los assets de visualización ya no contienen código hardcodeado de plotnine sino que ejecutan el código generado por la IA.
- `checks_files.py`: sin cambios respecto a la Práctica 3.
- `definitions.py`: manifiesto central de Dagster actualizado con el job del pipeline completo y el sensor de detección de cambios en los ficheros de datos.

**Gráficos generados automáticamente:**
- `estructura_ingresos_isla_2023.png`: composición porcentual de ingresos por isla en Canarias (2023).
- `evolucion_estructura_islas.png`: evolución de la estructura de ingresos por isla (2015-2023).
- `boxplot_evolucion_temporal.png`: variación del peso de los sueldos entre municipios por isla (2015-2023).
- `heatmap_municipios_tenerife.png`: estructura de ingresos en municipios de Tenerife (2023).
- `scatter_educacion_desempleo.png`: relación entre educación superior y prestaciones por desempleo (2023).
- `mapa_desempleo_municipios.png`: mapa coroplético de prestaciones por desempleo por municipio en Canarias (2023).

### Funcionamiento del pipeline
El pipeline sigue el siguiente flujo para cada gráfico:
```
Carga → Transformación → Template IA → Generación de código → Visualización
```
El asset de plantilla construye el prompt describiendo el gráfico según la gramática de gráficos de Wickham. El asset de generación de código envía el prompt al servidor de IA de la ULL (`gpu1.esit.ull.es`) y limpia el código recibido. El asset de visualización ejecuta el código con `exec()` y sube la imagen resultante a GitHub automáticamente.

### Automatización
Se ha añadido un sensor en Dagster que vigila los ficheros de datos (`distribucion-renta-canarias.csv`, `codislas.csv`, `nivelestudios.xlsx`). Cuando detecta un cambio en cualquiera de ellos lanza automáticamente el pipeline completo.

### Visualizaciones públicas
Las imágenes generadas están disponibles públicamente en GitHub Pages:
```
https://yaizavicenteperez.github.io/practica2-v/
```

### Paquetes utilizados
- Dagster
- Pandas
- Plotnine
- Geopandas
- Requests
- Openpyxl
- os
- re
- subprocess
