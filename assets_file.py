from dagster import Definitions, asset
import pandas as pd
from plotnine import *

# Cambiar a True para forzar fallos en los checks (paso 8)
FORZAR_FALLOS = True

# ASSETS DE CARGA DE DATOS ========================================
@asset
def raw_renta_data():
    """Carga los datos crudos de distribución de renta (distribucion-renta-canarias.csv)"""
    df = pd.read_csv('distribucion-renta-canarias.csv')

    # Añadido para forzar fallo de los checks
    if FORZAR_FALLOS:
        # Fuerza fallo de check_nulos_renta
        df.loc[62.2, 'OBS_VALUE'] = None
        # Fuerza fallo de check_columnas_renta
        df = df.drop(columns=['TERRITORIO_CODE'])
        # Fuerza fallo de check_n_tipos_ingreso
        df.loc[0, 'MEDIDAS#es'] = 'Tipo_inventado'
        # Fuerza fallo de check_suma_porcentajes
        df.loc[0, 'OBS_VALUE'] = 999
        # Fuerza fallo de check_n_categorias_color
        for i in range(8):
            df.loc[i, 'MEDIDAS#es'] = f'Tipo_extra_{i}'

    return df



@asset
def raw_codislas_data():
    """Carga y prepara los códigos de islas y municipios (codislas.csv)"""
    cod = pd.read_csv('codislas.csv', encoding='latin-1', sep=';')
    # Crear código completo de municipio (CPRO + CMUN)
    cod['CODIGO_MUNICIPIO'] = cod['CPRO'].astype(str) + cod['CMUN'].astype(str).str.zfill(3)

    # Añadido para forzar fallo de los checks
    if FORZAR_FALLOS:
        cod.loc[0, 'ISLA'] = None

    return cod[['CODIGO_MUNICIPIO', 'NOMBRE', 'ISLA']]


@asset
def raw_estudios_data():
    """Carga los datos de nivel de estudios (nivelestudios.xlsx)"""
    df_estudios = pd.read_excel('nivelestudios.xlsx')

    # Añadido para forzar fallo de los checks
    if FORZAR_FALLOS:
        df_estudios.loc[0, 'Nivel de estudios en curso'] = None

    return df_estudios


# ASSETS DE TRANSFORMACIÓN ===================================
# Archivo distribucion-renta-canarias.csv --------------------
@asset
def islas_agregadas_data(raw_renta_data):
    """Extrae datos de islas ya agregados del dataset original"""
    df_renta = raw_renta_data.copy()
    # Las 7 islas como vienen en el dataset
    islas = ['El Hierro', 'Fuerteventura', 'Gran Canaria',
             'La Gomera', 'La Palma', 'Lanzarote', 'Tenerife']
    df_islas_agregadas = df_renta[df_renta['TERRITORIO#es'].isin(islas)]

    # Añadido para forzar fallo de los checks
    if FORZAR_FALLOS:
        df_islas_agregadas = df_islas_agregadas[
            df_islas_agregadas['TERRITORIO#es'] != 'Tenerife'
            ]

    return df_islas_agregadas


@asset
def datos_estructura_isla_2023(islas_agregadas_data):
    """Prepara datos para gráfico de estructura porcentual por isla (2023)"""
    df_islas_2023 = islas_agregadas_data[islas_agregadas_data['TIME_PERIOD#es'] == 2023].copy()
    df_estructura = df_islas_2023.groupby(['TERRITORIO#es', 'MEDIDAS#es'], as_index=False)['OBS_VALUE'].sum()
    df_estructura.columns = ['Isla', 'Tipo_Ingreso', 'Porcentaje']
    return df_estructura


@asset
def datos_evolucion_temporal(islas_agregadas_data):
    """Prepara datos para area chart de evolución temporal por isla"""
    df_evol = islas_agregadas_data.groupby(
        ['TERRITORIO#es', 'TIME_PERIOD#es', 'MEDIDAS#es'],
        as_index=False
    )['OBS_VALUE'].sum()
    df_evol.columns = ['Isla', 'Año', 'Tipo_Ingreso', 'Porcentaje']

    # Ordenar tipos de ingreso
    orden_medidas = ['Sueldos y salarios', 'Pensiones', 'Prestaciones por desempleo',
                     'Otros ingresos', 'Otras prestaciones']
    df_evol['Tipo_Ingreso'] = pd.Categorical(
        df_evol['Tipo_Ingreso'],
        categories=orden_medidas,
        ordered=True
    )

    # Añadido para forzar fallo de los checks
    if FORZAR_FALLOS:
        # Fuerza fallo de check_continuidad_temporal eliminando un año
        df_evol = df_evol[df_evol['Año'] != 2019]

    return df_evol


# Archivo codislas.csv -------------------------------------
@asset
def municipios_data(raw_renta_data):
    """Filtra solo municipios (excluye agregados regionales e insulares)"""
    df_renta = raw_renta_data.copy()
    # Filtrar solo códigos de municipio (5 dígitos numéricos)
    df_mun = df_renta[df_renta['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)]
    return df_mun


@asset
def renta_con_isla(municipios_data, raw_codislas_data):
    """Une los datos de renta con información de isla mediante merge"""
    df_merge = municipios_data.merge(
        raw_codislas_data,
        left_on='TERRITORIO_CODE',
        right_on='CODIGO_MUNICIPIO',
        how='left'
    )
    return df_merge


@asset
def datos_boxplot_temporal(renta_con_isla):
    """Prepara datos para boxplot de evolución temporal"""
    años_comparar = [2015, 2019, 2023]
    df_boxplot = renta_con_isla[
        (renta_con_isla['TIME_PERIOD#es'].isin(años_comparar)) &
        (renta_con_isla['MEDIDAS#es'] == 'Sueldos y salarios')
    ].copy()
    df_boxplot['Año'] = df_boxplot['TIME_PERIOD#es'].astype(str)
    return df_boxplot


@asset
def datos_heatmap_tenerife(renta_con_isla):
    """Prepara datos para heatmap de municipios de Tenerife"""
    df_tenerife = renta_con_isla[
        (renta_con_isla['ISLA'] == 'Tenerife') &
        (renta_con_isla['TIME_PERIOD#es'] == 2023)
    ].copy()

    # Abreviar nombres de medidas
    abreviaturas_medidas = {
        'Sueldos y salarios': 'Sueldos',
        'Pensiones': 'Pensiones',
        'Prestaciones por desempleo': 'Desempleo',
        'Otros ingresos': 'Otros',
        'Otras prestaciones': 'Otras prest.'
    }
    df_tenerife['Tipo_Ingreso'] = df_tenerife['MEDIDAS#es'].map(abreviaturas_medidas)
    df_tenerife = df_tenerife.sort_values('NOMBRE')

    # Añadido para forzar fallo de los checks
    if FORZAR_FALLOS:
        # Fuerza fallo de check_municipios_tenerife eliminando algunos municipios
        df_tenerife = df_tenerife[df_tenerife['NOMBRE'] != 'Santa Cruz de Tenerife']

    return df_tenerife

# Archivo nivelestudios.xlsx -----------------------------

@asset
def datos_educacion_procesados(raw_estudios_data):
    """Procesa datos de estudios: calcula % educación superior por municipio (2023)"""
    df_est = raw_estudios_data.copy()

    # Extraer código de municipio
    df_est['CODIGO_MUN'] = df_est['Municipios de 500 habitantes o más'].str.split().str[0]

    # Filtrar año 2023, Total sexo
    df_est_2023 = df_est[
        (df_est['Periodo'] == '2023-01-01') &
        (df_est['Sexo'] == 'Total')
    ].copy()

    # Agrupar por municipio y nivel educativo (suma española + extranjera)
    df_est_agrupado = df_est_2023.groupby(
        ['CODIGO_MUN', 'Nivel de estudios en curso'],
        as_index=False
    )['Total'].sum()

    # Población con educación superior
    df_est_superior = df_est_agrupado[
        df_est_agrupado['Nivel de estudios en curso'] == 'Educación superior'
    ].copy()
    df_est_superior.columns = ['CODIGO_MUN', 'Nivel', 'Poblacion_Superior']

    # Población total
    df_est_total = df_est_agrupado[
        df_est_agrupado['Nivel de estudios en curso'] == 'Total'
    ].copy()
    df_est_total.columns = ['CODIGO_MUN', 'Nivel', 'Poblacion_Total']

    # Merge y calcular porcentaje
    df_educacion = df_est_superior[['CODIGO_MUN', 'Poblacion_Superior']].merge(
        df_est_total[['CODIGO_MUN', 'Poblacion_Total']],
        on='CODIGO_MUN'
    )
    df_educacion['Pct_Educacion_Superior'] = (
            df_educacion['Poblacion_Superior'] / df_educacion['Poblacion_Total'] * 100
    )

    return df_educacion

@asset
def datos_educacion_renta(datos_educacion_procesados, renta_con_isla):
    """Combina datos de educación con datos de renta por municipio"""

    # Filtrar renta 2023, solo DESEMPLEO
    df_renta_desempleo_2023 = renta_con_isla[
        (renta_con_isla['TIME_PERIOD#es'] == 2023) &
        (renta_con_isla['MEDIDAS#es'] == 'Prestaciones por desempleo')  # ← CAMBIO AQUÍ
    ].copy()

    # Merge educación + renta
    df_edu_renta = datos_educacion_procesados.merge(
        df_renta_desempleo_2023[['TERRITORIO_CODE', 'OBS_VALUE', 'NOMBRE', 'ISLA']],
        left_on='CODIGO_MUN',
        right_on='TERRITORIO_CODE',
        how='inner'
    )

    df_edu_renta = df_edu_renta.rename(columns={'OBS_VALUE': 'Pct_Desempleo'})  # ← CAMBIO AQUÍ

    return df_edu_renta



# ASSETS DE VISUALIZACIÓN ========================================
# Con archivo distribucion-renta-canarias.csv --------------------
@asset
def viz_estructura_isla_2023(datos_estructura_isla_2023):
    """Genera gráfico de barras apiladas: estructura porcentual por isla (2023)"""
    df_viz = datos_estructura_isla_2023

    grafico = (
            ggplot(df_viz, aes(x='Isla', y='Porcentaje', fill='Tipo_Ingreso'))
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

    filename = 'estructura_ingresos_isla_2023.png'
    grafico.save(filename, dpi=300, width=14, height=8)

    # Añadido para forzar fallo de los checks
    if FORZAR_FALLOS:
        # Fuerza fallo de check_archivo_png_generado
        filename = 'archivo_inexistente.png'
    return filename


@asset
def viz_evolucion_temporal(datos_evolucion_temporal):
    """Genera area chart facetado: evolución temporal por isla"""
    df_viz_evol = datos_evolucion_temporal

    grafico = (
            ggplot(df_viz_evol, aes(x='Año', y='Porcentaje', fill='Tipo_Ingreso'))
            + geom_area(alpha=0.8)
            + facet_wrap('~Isla', ncol=2, scales='fixed')
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

    filename = 'evolucion_estructura_islas.png'
    grafico.save(filename, dpi=300, width=16, height=14)
    return filename


# Con archivos distribucion-renta-canarias.csv y codislas.csv ----------
@asset
def viz_boxplot_temporal(datos_boxplot_temporal):
    """Genera boxplot comparando evolución temporal de sueldos por isla"""

    df_viz_box = datos_boxplot_temporal

    grafico = (
            ggplot(df_viz_box, aes(x='ISLA', y='OBS_VALUE', fill='Año'))
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

    filename = 'boxplot_evolucion_temporal.png'
    grafico.save(filename, dpi=300, width=15, height=8)
    return filename


@asset
def viz_heatmap_tenerife(datos_heatmap_tenerife):
    """Genera heatmap de estructura de ingresos en municipios de Tenerife"""

    df_viz_heat = datos_heatmap_tenerife

    grafico = (
            ggplot(df_viz_heat, aes(x='Tipo_Ingreso', y='NOMBRE', fill='OBS_VALUE'))
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
        title='Estructura de ingresos en municipios de Tenerife (2023)',
        x='Tipo de ingreso',
        y='Municipio',
        fill='Porcentaje\n(%)',
        caption='Fuente: ISTAC'
    )
            + scale_fill_gradient2(
        low='#ecf0f1',
        mid='#3498db',
        high='#e74c3c',
        midpoint=50
    )
    )

    filename = 'heatmap_municipios_tenerife.png'
    grafico.save(filename, dpi=300, width=10, height=18)
    return filename

# Con archivos distribucion-renta-canarias.csv, codislas.csv y nivelestudios.xlsx ----------

@asset
def viz_scatter_educacion_desempleo(datos_educacion_renta):  # ← CAMBIO NOMBRE
    """Genera scatter plot: correlación entre educación superior y dependencia de prestaciones por desempleo"""

    df_viz_scatter = datos_educacion_renta

    # Calcular correlación
    correlacion = df_viz_scatter[['Pct_Educacion_Superior', 'Pct_Desempleo']].corr().iloc[0, 1]

    # Crear gráfico
    grafico = (
            ggplot(df_viz_scatter, aes(x='Pct_Educacion_Superior', y='Pct_Desempleo', color='ISLA'))
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
        subtitle=f'Cada punto = municipio. Correlación = {correlacion:.3f}',
        x='Población con educación superior (%)',
        y='Peso de prestaciones por desempleo en la renta (%)',
        color='Isla',
        caption='Fuente: ISTAC'
    )
            + scale_color_brewer(type='qual', palette='Set2')
    )

    filename = 'scatter_educacion_desempleo.png'
    grafico.save(filename, dpi=300, width=14, height=9)
    return filename




