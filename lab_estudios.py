import pandas as pd
from plotnine import *

# CARGAR DATOS ===================================

df_estudios = pd.read_excel('nivelestudios.xlsx')
df_renta = pd.read_csv('distribucion-renta-canarias.csv')
cod = pd.read_csv('codislas.csv', encoding='latin-1', sep=';')

# Preparar codislas
cod['CODIGO_MUNICIPIO'] = cod['CPRO'].astype(str) + cod['CMUN'].astype(str).str.zfill(3)
cod_clean = cod[['CODIGO_MUNICIPIO', 'NOMBRE', 'ISLA']]


# PREPARAR DATOS ================================
# Datos estudios
# Extraer código de municipio
df_estudios['CODIGO_MUN'] = df_estudios['Municipios de 500 habitantes o más'].str.split().str[0]

# Filtrar año 2023, Total sexo
df_estudios_2023 = df_estudios[
    (df_estudios['Periodo'] == '2023-01-01') &
    (df_estudios['Sexo'] == 'Total')
].copy()

# Agrupar por municipio y nivel educativo (suma española + extranjera)
df_estudios_agrupado = df_estudios_2023.groupby(
    ['CODIGO_MUN', 'Nivel de estudios en curso'],
    as_index=False
)['Total'].sum()

# Calcular población con educación superior
df_superior = df_estudios_agrupado[
    df_estudios_agrupado['Nivel de estudios en curso'] == 'Educación superior'
].copy()
df_superior.columns = ['CODIGO_MUN', 'Nivel', 'Poblacion_Superior']

# Calcular población total
df_total_edu = df_estudios_agrupado[
    df_estudios_agrupado['Nivel de estudios en curso'] == 'Total'
].copy()
df_total_edu.columns = ['CODIGO_MUN', 'Nivel', 'Poblacion_Total']

# Merge y calcular porcentaje
df_edu_procesado = df_superior[['CODIGO_MUN', 'Poblacion_Superior']].merge(
    df_total_edu[['CODIGO_MUN', 'Poblacion_Total']],
    on='CODIGO_MUN'
)
df_edu_procesado['Pct_Educacion_Superior'] = (
    df_edu_procesado['Poblacion_Superior'] / df_edu_procesado['Poblacion_Total'] * 100
)


# Datos renta (DESEMPLEO) --------------------------------
# Filtrar municipios
df_renta_mun = df_renta[df_renta['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)].copy()

# Filtrar 2023, PRESTACIONES POR DESEMPLEO
df_renta_2023 = df_renta_mun[
    (df_renta_mun['TIME_PERIOD#es'] == 2023) &
    (df_renta_mun['MEDIDAS#es'] == 'Prestaciones por desempleo')
].copy()



# Combinar datos ----------------------------
# Merge educación + renta
df_final = df_edu_procesado.merge(
    df_renta_2023[['TERRITORIO_CODE', 'OBS_VALUE']],
    left_on='CODIGO_MUN',
    right_on='TERRITORIO_CODE',
    how='inner'
)

# Renombrar columna de renta
df_final = df_final.rename(columns={'OBS_VALUE': 'Pct_Desempleo'})

# Añadir info de isla
df_final = df_final.merge(
    cod_clean,
    left_on='CODIGO_MUN',
    right_on='CODIGO_MUNICIPIO',
    how='left'
)


# SCATTER PLOT ========================================

# Calcular correlación
correlacion = df_final[['Pct_Educacion_Superior', 'Pct_Desempleo']].corr().iloc[0, 1]
print(f"\nCorrelación: {correlacion:.3f}")

# Crear gráfico
grafico_scatter = (
    ggplot(df_final, aes(x='Pct_Educacion_Superior', y='Pct_Desempleo', color='ISLA'))
    + geom_point(size=3.5, alpha=0.7)
    + geom_smooth(method='lm', se=False, color='black', linetype='dashed', size=0.8)
    + theme_minimal()
    + theme(
        figure_size=(14, 9),
        plot_title=element_text(size=14, weight='bold'),
        legend_position='right'
    )
    + labs(
        title='Relación entre educación superior y dependencia de prestaciones por desempleo (2023)',
        subtitle=f'Correlación = {correlacion:.3f}',
        x='Población con educación superior (%)',
        y='Peso de prestaciones por desempleo en la renta (%)',
        color='Isla',
        caption='Fuente: ISTAC'
    )
    + scale_color_brewer(type='qual', palette='Set2')
)

grafico_scatter.save('scatter_educacion_desempleo.png', dpi=300, width=14, height=9)


print("1. scatter_educacion_desempleo.png")