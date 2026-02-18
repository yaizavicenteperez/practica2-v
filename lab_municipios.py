import pandas as pd
from plotnine import *

# CARGAR DATOS ================================

df = pd.read_csv('distribucion-renta-canarias.csv')
cod = pd.read_csv('codislas.csv', encoding='latin-1', sep=';')

# Preparar codislas
cod['CODIGO_MUNICIPIO'] = cod['CPRO'].astype(str) + cod['CMUN'].astype(str).str.zfill(3)
cod_clean = cod[['CODIGO_MUNICIPIO', 'NOMBRE', 'ISLA']]

# Filtrar municipios y hacer merge
df_municipios = df[df['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)].copy()
df_full = df_municipios.merge(
    cod_clean,
    left_on='TERRITORIO_CODE',
    right_on='CODIGO_MUNICIPIO',
    how='left'
)



# GRÁFICO 1 - BOXPLOT EVOLUCIÓN TEMPORAL ==================================

# Filtrar años y tipo de medida
años_comparar = [2015, 2019, 2023]
df_box = df_full[
    (df_full['TIME_PERIOD#es'].isin(años_comparar)) &
    (df_full['MEDIDAS#es'] == 'Sueldos y salarios')
].copy()

# Convertir año a string
df_box['Año'] = df_box['TIME_PERIOD#es'].astype(str)

# Crear boxplot
grafico_boxplot = (
    ggplot(df_box, aes(x='ISLA', y='OBS_VALUE', fill='Año'))
    + geom_boxplot(position='dodge', alpha=0.7)
    + theme_minimal()
    + theme(
        figure_size=(15, 8),
        axis_text_x=element_text(rotation=45, hjust=1, size=10),
        plot_title=element_text(size=14, weight='bold'),
        legend_position='top'
    )
    + labs(
        title='Variación del peso de los sueldos entre municipios por isla (2015-2023)',
        x='Isla',
        y='Sueldos y salarios',
        fill='Año',
        caption='Fuente: ISTAC'
    )
    + scale_fill_manual(values=['#3498db', '#f39c12', '#e74c3c'])
)

grafico_boxplot.save('boxplot_evolucion_temporal.png', dpi=300, width=15, height=8)


# GRÁFICO 2: HEATMAP =================================

# Filtrar solo Tenerife, año 2023
df_heat = df_full[
    (df_full['ISLA'] == 'Tenerife') &
    (df_full['TIME_PERIOD#es'] == 2023)
].copy()


# Abreviar nombres de medidas para que quepan mejor
abreviaturas_medidas = {
    'Sueldos y salarios': 'Sueldos',
    'Pensiones': 'Pensiones',
    'Prestaciones por desempleo': 'Desempleo',
    'Otros ingresos': 'Otros',
    'Otras prestaciones': 'Otras prest.'
}

df_heat['Tipo_Ingreso'] = df_heat['MEDIDAS#es'].map(abreviaturas_medidas)

# Ordenar municipios alfabéticamente
df_heat = df_heat.sort_values('NOMBRE')

# Crear heatmap
grafico_heatmap = (
    ggplot(df_heat, aes(x='Tipo_Ingreso', y='NOMBRE', fill='OBS_VALUE'))
    + geom_tile(color='white', size=1.5)
    + geom_text(aes(label='round(OBS_VALUE, 1)'), size=7, color='black', fontweight='bold')
    + theme_minimal()
    + theme(
        figure_size=(10, 18),
        axis_text_x=element_text(rotation=0, hjust=0.5, size=10, weight='bold'),
        axis_text_y=element_text(size=9),
        plot_title=element_text(size=14, weight='bold'),
        legend_position='right'
    )
    + labs(
        title='Estructura de ingresos en municipios de tenerife (2023)',
        x='Tipo de ingreso',
        y='Municipio',
        fill='Porcentaje\n(%)',
        caption='Fuente: ISTAC'
    )
    + scale_fill_gradient2(
        low='#ecf0f1',      # Gris claro para valores bajos
        mid='#3498db',      # Azul para valores medios
        high='#e74c3c',     # Rojo para valores altos
        midpoint=50
    )
)

grafico_heatmap.save('heatmap_municipios_tenerife.png', dpi=300, width=10, height=18)


print("1. boxplot_evolucion_temporal.png")
print("2. ranking_municipios_sueldos.png")