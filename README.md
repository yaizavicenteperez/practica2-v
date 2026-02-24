# practica3-v
Práctica 3 Visualización: Calidad de la Visualización. Checks

## Descripción
Se trabaja sobre el trabajo anterior, que encontramos en la rama principal de este proyecto. Aquí comentaremos únicamente los archivos nuevos y los cambios que se han realizado. Se ha mantenido la misma funcionalidad pero se han añadido checks en differentes assets a lo largo del pipeline. 


## Estructura del proyecto (archivos nuevos o modificados)
Programas:
- assets_file.py: pipeline de assets de Dagster, organizados en tres etapas: carga, transformación y visualización.
- checks_files.py: checks de calidad del pipeline, organizados en tres etapas: carga, transformación y visualización.
- definitions.py: manifiesto central de Dagster que registra los assets y checks del pipeline.
  

## Paquetes utilizados
- Dagster
- Pandas
- Plotnine
- Openpyxl
- os
