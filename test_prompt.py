import re, requests, pandas as pd, subprocess
from dagster import asset, asset_check, Output, AssetCheckResult, MetadataValue
from plotnine import *
import os


@asset
def islas_raw():
    df = pd.read_csv("./pwbi-1.csv")
    return Output(
        value=df,
        metadata={"variables": MetadataValue.json(list(df.columns)), "mensaje": "columnas del dataset"}
    )


@asset_check(asset=islas_raw)
def check_estandarizacion_islas(islas_raw):
    originales = islas_raw['isla'].nunique()
    normalizadas = islas_raw['isla'].str.capitalize().nunique()

    passed = originales == normalizadas

    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas": MetadataValue.int(normalizadas),
            "principio_gestalt": "Similitud (Evitar fragmentación visual)",
            "mensaje": "Si hay nombres inconsistentes, ggplot creará leyendas duplicadas."
        }
    )


@asset
def template_ia(islas_raw):
    columnas = ", ".join(islas_raw.columns)
    islas = islas_raw['isla'].unique().tolist()
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
    - Dataset: islas_raw
    - Estéticas: 
        * Variable 'año' mapeada al eje X.
        * Variable 'valor' mapeada al eje Y.
        * Una geometría de línea independiente para cada 'isla' (color/group).
    - Geometría: Línea (geom_line).
    - Etiquetas: 
        * Título: 'Evolución del Gasto por Isla'.
        * Eje Y: 'Gasto en €'.
    - Principio Gestalt (Punto Focal): 
        * Resaltar 'Tenerife'.
        * Resto de islas en gris claro (#D3D3D3).
        * Usar scale_color_manual para definir estos colores.
    )
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
def codigo_generado_ia(context, template_ia):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}

    try:
        response = requests.post(url, json=template_ia, headers=headers, timeout=60)
        response.raise_for_status()

        res_json = response.json()
        codigo_raw = res_json['choices'][0]['message']['content']

        match = re.search(r"```python\s+(.*?)\s+```", codigo_raw, re.DOTALL)

        if match:
            codigo_final = match.group(1)
        else:
            lineas_validas = []
            for l in codigo_raw.split("\n"):
                if not l.strip().startswith("###") and not l.strip().startswith("-"):
                    lineas_validas.append(l)
            codigo_final = "\n".join(lineas_validas)

        codigo_final = codigo_final.strip()

        return Output(
            value=codigo_final,
            metadata={
                "codigo_completo": MetadataValue.md(f"```python\n{codigo_final}\n```")
            }
        )

    except Exception as e:
        context.log.error(f"Error en la petición: {e}")
        raise e


@asset
def visualizacion_png(context, codigo_generado_ia, islas_raw):
    import plotnine
    df = islas_raw
    entorno_ejecucion = globals().copy()
    entorno_ejecucion['plotnine'] = plotnine
    entorno_ejecucion.update({
        k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')
    })
    entorno_ejecucion['pd'] = pd
    try:
        exec(codigo_generado_ia, entorno_ejecucion)

        grafico = entorno_ejecucion['generar_plot'](islas_raw)

        ruta_archivo = os.path.join(os.path.dirname(os.path.abspath(__file__)), "visualizacion_ia_1.png")
        grafico.save(ruta_archivo, width=10, height=6, dpi=100)
        subprocess.run(["git", "add", ruta_archivo])
        subprocess.run(["git", "commit", "-m", "Actualización automática del gráfico"])
        subprocess.run(["git", "push"])
        return Output(
            value=ruta_archivo,
            metadata={"ruta": ruta_archivo, "mensaje": "Gráfico generado y guardado"}
        )
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise e