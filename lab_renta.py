import pandas as pd
from plotnine import *


# CARGAR DATOS ========================================

df = pd.read_csv('distribucion-renta-canarias-2023.csv')

# GRÁFICO 1 - DISTRIBUCIÓN POR ISLA =====================

# Filtrar solo las 7 islas (datos del CSV)
islas = ['El Hierro', 'Fuerteventura', 'Gran Canaria',
         'La Gomera', 'La Palma', 'Lanzarote', 'Tenerife']

df_islas = df[df['TERRITORIO#es'].isin(islas)].copy()

# Filtrar año 2023
df_2023 = df_islas[df_islas['TIME_PERIOD#es'] == 2023].copy()

# Agrupar
df_plot = df_2023.groupby(['TERRITORIO#es', 'MEDIDAS#es'], as_index=False)['OBS_VALUE'].sum()
df_plot.columns = ['Isla', 'Tipo_Ingreso', 'Porcentaje']


# Crear gráfico de barras apiladas
grafico_barras = (
    ggplot(df_plot, aes(x='Isla', y='Porcentaje', fill='Tipo_Ingreso'))
    + geom_col(position='stack')
    + theme_minimal()
    + theme(
        figure_size=(14, 8),
        axis_text_x=element_text(rotation=45, hjust=1, size=11),
        plot_title=element_text(size=14, weight='bold'),
        legend_position='right'
    )
    + labs(
        title='Composición porcentual de ingresos por isla en Canarias (2023)',
        x='Isla',
        y='Distribución porcentual (%)',
        fill='Tipo de ingreso',
        caption='Fuente: ISTAC'
    )
    + scale_fill_brewer(type='qual', palette='Set2')
)

grafico_barras.save('estructura_ingresos_isla_2023.png', dpi=300, width=14, height=8)


# GRÁFICO 2 - AREA CHART ===================================

# Todas las islas, todos los años
df_evolucion = df_islas.groupby(['TERRITORIO#es', 'TIME_PERIOD#es', 'MEDIDAS#es'],
                                 as_index=False)['OBS_VALUE'].sum()
df_evolucion.columns = ['Isla', 'Año', 'Tipo_Ingreso', 'Porcentaje']

# Ordenar tipos de ingreso para apilado coherente
orden_medidas = ['Sueldos y salarios', 'Pensiones', 'Prestaciones por desempleo',
                 'Otros ingresos', 'Otras prestaciones']
df_evolucion['Tipo_Ingreso'] = pd.Categorical(
    df_evolucion['Tipo_Ingreso'],
    categories=orden_medidas,
    ordered=True
)

# Crear area chart facetado por isla
grafico_area = (
    ggplot(df_evolucion, aes(x='Año', y='Porcentaje', fill='Tipo_Ingreso'))
    + geom_area(alpha=0.8)
    + facet_wrap('~Isla', ncol=2, scales='fixed')  # Escala fija para comparar
    + theme_minimal()
    + theme(
    figure_size=(16, 14),
    plot_title=element_text(size=14, weight='bold'),
    strip_text=element_text(size=11, weight='bold'),
    legend_position='bottom',
    legend_direction='horizontal'
)
    + labs(
        title='Evolución de la estructura de ingresos por isla (2015-2023)',
        x='Año',
        y='Distribución porcentual (%)',
        fill='Tipo de ingreso',
        caption='Fuente: ISTAC'
    )
    + scale_x_continuous(breaks=[2015, 2019, 2023])
    + scale_fill_brewer(type='qual', palette='Set2')
)

grafico_area.save('evolucion_estructura_islas.png', dpi=300, width=16, height=14)


print("1. estructura_ingresos_isla_2023.png")
print("2. evolucion_estructura_islas.png")

