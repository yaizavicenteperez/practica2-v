import re, requests, pandas as pd, subprocess, os
from dagster import asset, Output, MetadataValue
from plotnine import *


FORZAR_FALLOS = False

# FUNCIÓN AUXILIAR ========================================
def llamar_servicio_ia(template):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}

    response = requests.post(url, json=template, headers=headers, timeout=60)
    response.raise_for_status()

    codigo_raw = response.json()['choices'][0]['message']['content']

    match = re.search(r"```python\s+(.*?)\s+```", codigo_raw, re.DOTALL)
    if match:
        return match.group(1).strip()
    else:
        lineas_validas = []
        for l in codigo_raw.split("\n"):
            if not l.strip().startswith("###") and not l.strip().startswith("-"):
                lineas_validas.append(l)
        return "\n".join(lineas_validas).strip()


def ejecutar_codigo_ia(codigo, df):
    """Ejecuta el código generado por la IA en un entorno controlado"""
    import plotnine
    entorno = globals().copy()
    entorno['plotnine'] = plotnine
    entorno.update({
        k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')
    })
    entorno['pd'] = pd
    entorno['df'] = df
    exec(codigo, entorno)
    return entorno['generar_plot'](df)


def subir_a_github(ruta_archivo):
    """Sube el archivo generado a GitHub"""
    subprocess.run(["git", "add", ruta_archivo])
    subprocess.run(["git", "commit", "-m", f"Actualización automática: {ruta_archivo}"])
    subprocess.run(["git", "push"])


# ASSETS DE CARGA ========================================
@asset
def raw_renta_data():
    """Carga los datos crudos de distribución de renta (distribucion-renta-canarias.csv)"""
    df = pd.read_csv('distribucion-renta-canarias.csv')
    return df


@asset
def raw_codislas_data():
    """Carga y prepara los códigos de islas y municipios (codislas.csv)"""
    cod = pd.read_csv('codislas.csv', encoding='latin-1', sep=';')
    cod['CODIGO_MUNICIPIO'] = cod['CPRO'].astype(str) + cod['CMUN'].astype(str).str.zfill(3)
    return cod[['CODIGO_MUNICIPIO', 'NOMBRE', 'ISLA']]


@asset
def raw_estudios_data():
    """Carga los datos de nivel de estudios (nivelestudios.xlsx)"""
    df_estudios = pd.read_excel('nivelestudios.xlsx')
    return df_estudios


# ASSETS DE TRANSFORMACIÓN ========================================
@asset
def islas_agregadas_data(raw_renta_data):
    """Extrae datos de islas ya agregados del dataset original"""
    df_renta = raw_renta_data.copy()
    islas = ['El Hierro', 'Fuerteventura', 'Gran Canaria',
             'La Gomera', 'La Palma', 'Lanzarote', 'Tenerife']
    return df_renta[df_renta['TERRITORIO#es'].isin(islas)]


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
    orden_medidas = ['Sueldos y salarios', 'Pensiones', 'Prestaciones por desempleo',
                     'Otros ingresos', 'Otras prestaciones']
    df_evol['Tipo_Ingreso'] = pd.Categorical(
        df_evol['Tipo_Ingreso'],
        categories=orden_medidas,
        ordered=True
    )
    return df_evol


@asset
def municipios_data(raw_renta_data):
    """Filtra solo municipios (excluye agregados regionales e insulares)"""
    df_renta = raw_renta_data.copy()
    return df_renta[df_renta['TERRITORIO_CODE'].str.match(r'^\d{5}$', na=False)]


@asset
def renta_con_isla(municipios_data, raw_codislas_data):
    """Une los datos de renta con información de isla mediante merge"""
    return municipios_data.merge(
        raw_codislas_data,
        left_on='TERRITORIO_CODE',
        right_on='CODIGO_MUNICIPIO',
        how='left'
    )


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
    abreviaturas_medidas = {
        'Sueldos y salarios': 'Sueldos',
        'Pensiones': 'Pensiones',
        'Prestaciones por desempleo': 'Desempleo',
        'Otros ingresos': 'Otros',
        'Otras prestaciones': 'Otras prest.'
    }
    df_tenerife['Tipo_Ingreso'] = df_tenerife['MEDIDAS#es'].map(abreviaturas_medidas)
    return df_tenerife.sort_values('NOMBRE')


@asset
def datos_educacion_procesados(raw_estudios_data):
    """Procesa datos de estudios: calcula % educación superior por municipio (2023)"""
    df_est = raw_estudios_data.copy()
    df_est['CODIGO_MUN'] = df_est['Municipios de 500 habitantes o más'].str.split().str[0]
    df_est_2023 = df_est[
        (df_est['Periodo'] == '2023-01-01') &
        (df_est['Sexo'] == 'Total')
    ].copy()
    df_est_agrupado = df_est_2023.groupby(
        ['CODIGO_MUN', 'Nivel de estudios en curso'],
        as_index=False
    )['Total'].sum()
    df_est_superior = df_est_agrupado[
        df_est_agrupado['Nivel de estudios en curso'] == 'Educación superior'
    ].copy()
    df_est_superior.columns = ['CODIGO_MUN', 'Nivel', 'Poblacion_Superior']
    df_est_total = df_est_agrupado[
        df_est_agrupado['Nivel de estudios en curso'] == 'Total'
    ].copy()
    df_est_total.columns = ['CODIGO_MUN', 'Nivel', 'Poblacion_Total']
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
    df_renta_desempleo_2023 = renta_con_isla[
        (renta_con_isla['TIME_PERIOD#es'] == 2023) &
        (renta_con_isla['MEDIDAS#es'] == 'Prestaciones por desempleo')
    ].copy()
    df_edu_renta = datos_educacion_procesados.merge(
        df_renta_desempleo_2023[['TERRITORIO_CODE', 'OBS_VALUE', 'NOMBRE', 'ISLA']],
        left_on='CODIGO_MUN',
        right_on='TERRITORIO_CODE',
        how='inner'
    )
    return df_edu_renta.rename(columns={'OBS_VALUE': 'Pct_Desempleo'})


# ASSETS DE PLANTILLAS IA ========================================
@asset
def template_ia_estructura(datos_estructura_isla_2023):
    """Plantilla para gráfico de barras apiladas de estructura porcentual por isla"""
    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura:
    # plot = (ggplot(df, aes(...)) + geom_...)
    # return plot
"""
    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve exclusivamente el código Python."
    )
    descripcion_grafico = """
    - Dataset: df con columnas ['Isla', 'Tipo_Ingreso', 'Porcentaje']
    - Geometría: Barras apiladas verticales (geom_col con position='stack')
    - Estéticas:
        * Variable 'Isla' mapeada al eje X (escala categórica discreta)
        * Variable 'Porcentaje' mapeada al eje Y (escala continua)
        * Variable 'Tipo_Ingreso' mapeada al fill (escala cualitativa)
    - Escalas:
        * Usa scale_fill_manual con values=['#66c2a5','#fc8d62','#8da0cb','#e78ac3','#a6d854']
          para los 5 tipos de ingreso. NO uses scale_fill_brewer.
    - Etiquetas:
        * Título: 'Composición porcentual de ingresos por isla en Canarias (2023)'
        * Eje X: 'Isla'
        * Eje Y: 'Distribución porcentual (%)'
        * Leyenda fill: 'Tipo de ingreso'
        * Caption: 'Fuente: ISTAC'
    - Tema: IMPORTANTE: el tamaño de figura va DENTRO de theme(), nunca en theme_minimal().
      Ejemplo correcto:
      + theme_minimal()
      + theme(figure_size=(14, 8), axis_text_x=element_text(rotation=45, hjust=1))
    """
    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion_grafico}"
    return {
        "model": "ollama/deepseek-coder:6.7b-instruct-q4_K_M",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


@asset
def template_ia_evolucion(datos_evolucion_temporal):
    """Plantilla para area chart facetado de evolución temporal por isla"""
    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura:
    # plot = (ggplot(df, aes(...)) + geom_...)
    # return plot
"""
    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve exclusivamente el código Python."
    )
    descripcion_grafico = """
    - Dataset: df con columnas ['Isla', 'Año', 'Tipo_Ingreso', 'Porcentaje']
    - Geometría: Áreas apiladas (geom_area con alpha=0.8)
    - Estéticas:
        * Variable 'Año' mapeada al eje X (escala continua, breaks en 2015, 2019, 2023)
        * Variable 'Porcentaje' mapeada al eje Y (escala continua)
        * Variable 'Tipo_Ingreso' mapeada al fill (escala cualitativa)
    - Facetas: facet_wrap por 'Isla' con ncol=2 y scales='fixed'
    - Escalas:
        * Usa scale_fill_manual con values=['#66c2a5','#fc8d62','#8da0cb','#e78ac3','#a6d854']
          para los 5 tipos de ingreso. NO uses scale_fill_brewer.
        * scale_x_continuous con breaks=[2015, 2019, 2023]
    - Etiquetas:
        * Título: 'Evolución de la estructura de ingresos por isla (2015-2023)'
        * Eje X: 'Año'
        * Eje Y: 'Distribución porcentual (%)'
        * Leyenda fill: 'Tipo de ingreso'
        * Caption: 'Fuente: ISTAC'
    - Tema: IMPORTANTE: el tamaño de figura va DENTRO de theme(), nunca en theme_minimal().
      Ejemplo correcto:
      + theme_minimal()
      + theme(figure_size=(16, 14), legend_position='bottom', legend_direction='horizontal')
    """
    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion_grafico}"
    return {
        "model": "ollama/deepseek-coder:6.7b-instruct-q4_K_M",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


@asset
def template_ia_boxplot(datos_boxplot_temporal):
    """Plantilla para boxplot de evolución temporal de sueldos por isla"""
    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura:
    # plot = (ggplot(df, aes(...)) + geom_...)
    # return plot
"""
    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve exclusivamente el código Python."
    )
    descripcion_grafico = """
    - Dataset: df con columnas ['ISLA', 'OBS_VALUE', 'Año']
    - Geometría: Cajas y bigotes agrupados (geom_boxplot con position='dodge' y alpha=0.7)
    - Estéticas:
        * Variable 'ISLA' mapeada al eje X (escala categórica)
        * Variable 'OBS_VALUE' mapeada al eje Y (escala continua)
        * Variable 'Año' mapeada al fill (escala manual)
    - Escalas:
        * scale_fill_manual con values=['#3498db', '#f39c12', '#e74c3c'] para los 3 años
    - Etiquetas:
        * Título: 'Variación del peso de los sueldos entre municipios por isla (2015-2023)'
        * Eje X: 'Isla'
        * Eje Y: 'Sueldos y salarios'
        * Leyenda fill: 'Año'
        * Caption: 'Fuente: ISTAC'
    - Tema: IMPORTANTE: el tamaño de figura va DENTRO de theme(), nunca en theme_minimal().
      Ejemplo correcto:
      + theme_minimal()
      + theme(figure_size=(15, 8), axis_text_x=element_text(rotation=45, hjust=1),
              legend_position='top')
    """
    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion_grafico}"
    return {
        "model": "ollama/deepseek-coder:6.7b-instruct-q4_K_M",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


@asset
def template_ia_heatmap(datos_heatmap_tenerife):
    """Plantilla para heatmap de municipios de Tenerife"""
    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura:
    # plot = (ggplot(df, aes(...)) + geom_...)
    # return plot
"""
    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve exclusivamente el código Python."
    )
    descripcion_grafico = """
    - Dataset: df con columnas ['Tipo_Ingreso', 'NOMBRE', 'OBS_VALUE']
    - Geometría:
        * Heatmap usando geom_tile(color='white', size=1.5) con fill mapeado a OBS_VALUE
        * Etiquetas numéricas dentro de cada celda usando geom_text.
          IMPORTANTE: el argumento label debe ir DENTRO del aes() del propio geom_text,
          no en el aes() global. Ejemplo correcto:
          geom_text(aes(label='OBS_VALUE.round(1)'), size=7, color='black')
    - Estéticas globales en ggplot(df, aes(...)):
        * Variable 'Tipo_Ingreso' mapeada al eje X (escala categórica)
        * Variable 'NOMBRE' mapeada al eje Y (escala categórica)
        * Variable 'OBS_VALUE' mapeada al fill (escala gradiente divergente)
    - Escalas:
        * scale_fill_gradient2 con low='#ecf0f1', mid='#3498db', high='#e74c3c', midpoint=50
    - Etiquetas:
        * Título: 'Estructura de ingresos en municipios de Tenerife (2023)'
        * Eje X: 'Tipo de ingreso'
        * Eje Y: 'Municipio'
        * Leyenda fill: 'Porcentaje (%)'
        * Caption: 'Fuente: ISTAC'
    - Tema: IMPORTANTE: el tamaño de figura va DENTRO de theme(), nunca en theme_minimal().
      Ejemplo correcto:
      + theme_minimal()
      + theme(figure_size=(10, 18), axis_text_x=element_text(hjust=0.5, weight='bold'),
              legend_position='right')
    """
    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion_grafico}"
    return {
        "model": "ollama/deepseek-coder:6.7b-instruct-q4_K_M",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


@asset
def template_ia_scatter(datos_educacion_renta):
    """Plantilla para scatter plot de correlación educación-desempleo"""
    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura:
    # plot = (ggplot(df, aes(...)) + geom_...)
    # return plot
"""
    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código ejecutable. "
        f"Usa siempre este template: {template_tecnico}. "
        "Devuelve exclusivamente el código Python."
    )
    descripcion_grafico = """
    - Dataset: df con columnas ['Pct_Educacion_Superior', 'Pct_Desempleo', 'ISLA']
    - Geometría:
        * Puntos (geom_point con size=3.5 y alpha=0.7)
        * Línea de tendencia lineal (geom_smooth con method='lm', se=False,
          color='black', linetype='dashed', size=0.8)
    - Estéticas:
        * Variable 'Pct_Educacion_Superior' mapeada al eje X (escala continua)
        * Variable 'Pct_Desempleo' mapeada al eje Y (escala continua)
        * Variable 'ISLA' mapeada al color (escala cualitativa)
    - Escalas:
        * Usa scale_color_manual con values=['#66c2a5','#fc8d62','#8da0cb','#e78ac3',
          '#a6d854','#ffd92f','#e5c494'] para las 7 islas.
          NO uses scale_color_brewer.
    - Etiquetas:
        * Título: 'Relación entre educación superior y dependencia de prestaciones por desempleo (2023)'
        * Eje X: 'Población con educación superior (%)'
        * Eje Y: 'Peso de prestaciones por desempleo en la renta (%)'
        * Leyenda color: 'Isla'
        * Caption: 'Fuente: ISTAC'
    - Tema: IMPORTANTE: el tamaño de figura va DENTRO de theme(), nunca en theme_minimal().
      Ejemplo correcto:
      + theme_minimal()
      + theme(figure_size=(14, 9), legend_position='right')
    """
    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion_grafico}"
    return {
        "model": "ollama/deepseek-coder:6.7b-instruct-q4_K_M",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


# ASSETS DE GENERACIÓN DE CÓDIGO IA ========================================
@asset
def codigo_generado_estructura(context, template_ia_estructura):
    codigo = llamar_servicio_ia(template_ia_estructura)
    return Output(
        value=codigo,
        metadata={"codigo": MetadataValue.md(f"```python\n{codigo}\n```")}
    )

@asset
def codigo_generado_evolucion(context, template_ia_evolucion):
    codigo = llamar_servicio_ia(template_ia_evolucion)
    return Output(
        value=codigo,
        metadata={"codigo": MetadataValue.md(f"```python\n{codigo}\n```")}
    )

@asset
def codigo_generado_boxplot(context, template_ia_boxplot):
    codigo = llamar_servicio_ia(template_ia_boxplot)
    return Output(
        value=codigo,
        metadata={"codigo": MetadataValue.md(f"```python\n{codigo}\n```")}
    )

@asset
def codigo_generado_heatmap(context, template_ia_heatmap):
    codigo = llamar_servicio_ia(template_ia_heatmap)
    return Output(
        value=codigo,
        metadata={"codigo": MetadataValue.md(f"```python\n{codigo}\n```")}
    )

@asset
def codigo_generado_scatter(context, template_ia_scatter):
    codigo = llamar_servicio_ia(template_ia_scatter)
    return Output(
        value=codigo,
        metadata={"codigo": MetadataValue.md(f"```python\n{codigo}\n```")}
    )


# ASSETS DE VISUALIZACIÓN ========================================
@asset
def viz_estructura_isla_2023(context, codigo_generado_estructura, datos_estructura_isla_2023):
    try:
        grafico = ejecutar_codigo_ia(codigo_generado_estructura, datos_estructura_isla_2023)
        ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "estructura_ingresos_isla_2023.png")
        grafico.save(ruta, dpi=300, width=14, height=8)
        subir_a_github(ruta)
        return Output(value=ruta, metadata={"ruta": ruta, "mensaje": "Gráfico generado y guardado"})
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise e

@asset
def viz_evolucion_temporal(context, codigo_generado_evolucion, datos_evolucion_temporal):
    try:
        grafico = ejecutar_codigo_ia(codigo_generado_evolucion, datos_evolucion_temporal)
        ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "evolucion_estructura_islas.png")
        grafico.save(ruta, dpi=300, width=16, height=14)
        subir_a_github(ruta)
        return Output(value=ruta, metadata={"ruta": ruta, "mensaje": "Gráfico generado y guardado"})
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise e

@asset
def viz_boxplot_temporal(context, codigo_generado_boxplot, datos_boxplot_temporal):
    try:
        grafico = ejecutar_codigo_ia(codigo_generado_boxplot, datos_boxplot_temporal)
        ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "boxplot_evolucion_temporal.png")
        grafico.save(ruta, dpi=300, width=15, height=8)
        subir_a_github(ruta)
        return Output(value=ruta, metadata={"ruta": ruta, "mensaje": "Gráfico generado y guardado"})
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise e

@asset
def viz_heatmap_tenerife(context, codigo_generado_heatmap, datos_heatmap_tenerife):
    try:
        grafico = ejecutar_codigo_ia(codigo_generado_heatmap, datos_heatmap_tenerife)
        ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "heatmap_municipios_tenerife.png")
        grafico.save(ruta, dpi=300, width=10, height=18)
        subir_a_github(ruta)
        return Output(value=ruta, metadata={"ruta": ruta, "mensaje": "Gráfico generado y guardado"})
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise e

@asset
def viz_scatter_educacion_desempleo(context, codigo_generado_scatter, datos_educacion_renta):
    try:
        grafico = ejecutar_codigo_ia(codigo_generado_scatter, datos_educacion_renta)
        ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scatter_educacion_desempleo.png")
        grafico.save(ruta, dpi=300, width=14, height=9)
        subir_a_github(ruta)
        return Output(value=ruta, metadata={"ruta": ruta, "mensaje": "Gráfico generado y guardado"})
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise e


@asset
def viz_mapa_desempleo_municipios(renta_con_isla):
    """Genera mapa coroplético de prestaciones por desempleo por municipio (2023)"""
    import geopandas as gpd
    from plotnine import ggplot, aes, geom_map, scale_fill_gradient2, labs, theme_void, theme, element_text

    # Cargar GeoJSON
    gdf = gpd.read_file('Municipios-2024.json')
    gdf['geocode'] = gdf['geocode'].astype(str).str.zfill(5)

    # Filtrar renta 2023, solo desempleo
    df_desempleo = renta_con_isla[
        (renta_con_isla['TIME_PERIOD#es'] == 2023) &
        (renta_con_isla['MEDIDAS#es'] == 'Prestaciones por desempleo')
    ].copy()
    df_desempleo['TERRITORIO_CODE'] = df_desempleo['TERRITORIO_CODE'].astype(str).str.zfill(5)

    # Merge GeoJSON con datos de renta
    gdf_merged = gdf.merge(
        df_desempleo[['TERRITORIO_CODE', 'OBS_VALUE', 'NOMBRE']],
        left_on='geocode',
        right_on='TERRITORIO_CODE',
        how='left'
    )

    # Generar mapa
    grafico = (
        ggplot(gdf_merged)
        + geom_map(aes(fill='OBS_VALUE'), color='white', size=0.1)
        + scale_fill_gradient2(
            low='#3498db',
            mid='#f5f5f5',
            high='#e74c3c',
            midpoint=gdf_merged['OBS_VALUE'].median(),
            na_value='#cccccc'
        )
        + labs(
            title='Prestaciones por desempleo por municipio en Canarias (2023)',
            fill='Porcentaje (%)',
            caption='Fuente: ISTAC'
        )
        + theme_void()
        + theme(
            figure_size=(16, 10),
            plot_title=element_text(size=14, weight='bold')
        )
    )

    ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapa_desempleo_municipios.png")
    grafico.save(ruta, dpi=300, width=16, height=10)
    subir_a_github(ruta)

    return Output(
        value=ruta,
        metadata={"ruta": ruta, "mensaje": "Mapa generado y guardado"}
    )