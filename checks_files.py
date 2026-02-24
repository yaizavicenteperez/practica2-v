from dagster import asset_check, AssetCheckResult, MetadataValue
import assets_file
import os

# CHECKS DE CARGA ========================================
@asset_check(asset=assets_file.raw_renta_data)
def check_nulos_renta(raw_renta_data):
    nulos = raw_renta_data['OBS_VALUE'].isna().sum()
    filas = len(raw_renta_data)
    return AssetCheckResult(
        passed=bool(nulos == 0),
        metadata={
            "pct_nulos": MetadataValue.float(float(nulos / filas * 100)),
            "filas_afectadas": MetadataValue.int(int(nulos))
        }
    )

@asset_check(asset=assets_file.raw_renta_data)
def check_columnas_renta(raw_renta_data):
    cols_esperadas = ['TERRITORIO_CODE', 'TERRITORIO#es', 'TIME_PERIOD#es', 'MEDIDAS#es', 'OBS_VALUE']
    faltantes = [c for c in cols_esperadas if c not in raw_renta_data.columns]
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        metadata={
            "columnas_faltantes": MetadataValue.text(str(faltantes))
        }
    )

@asset_check(asset=assets_file.raw_renta_data)
def check_n_tipos_ingreso(raw_renta_data):
    n_tipos = raw_renta_data['MEDIDAS#es'].nunique()
    return AssetCheckResult(
        passed=bool(n_tipos == 5),
        metadata={
            "n_tipos_detectados": MetadataValue.int(int(n_tipos))
        }
    )

@asset_check(asset=assets_file.raw_renta_data)
def check_suma_porcentajes(raw_renta_data):
    sumas = raw_renta_data.groupby(
        ['TERRITORIO_CODE', 'TIME_PERIOD#es']
    )['OBS_VALUE'].sum()
    incorrectos = sumas[(sumas < 99) | (sumas > 101)].index.tolist()
    return AssetCheckResult(
        passed=len(incorrectos) == 0,
        metadata={
            "municipios_incorrectos": MetadataValue.text(str(incorrectos[:5])),
            "suma_min": MetadataValue.float(float(sumas.min())),
            "suma_max": MetadataValue.float(float(sumas.max()))
        }
    )

@asset_check(asset=assets_file.raw_renta_data)
def check_n_categorias_color(raw_renta_data):
    n_cat = raw_renta_data['MEDIDAS#es'].nunique()
    return AssetCheckResult(
        passed=bool(n_cat <= 8),
        metadata={
            "n_categorias": MetadataValue.int(int(n_cat)),
            "sugerencia": MetadataValue.text("Máximo 8 categorías para paleta cualitativa")
        }
    )

@asset_check(asset=assets_file.raw_codislas_data)
def check_nulos_codislas(raw_codislas_data):
    nulos = raw_codislas_data[['CODIGO_MUNICIPIO', 'NOMBRE', 'ISLA']].isna().sum().sum()
    return AssetCheckResult(
        passed=bool(nulos == 0),
        metadata={
            "filas_afectadas": MetadataValue.int(int(nulos))
        }
    )

@asset_check(asset=assets_file.raw_estudios_data)
def check_nulos_estudios(raw_estudios_data):
    cols = ['Municipios de 500 habitantes o más', 'Periodo', 'Nivel de estudios en curso']
    nulos = raw_estudios_data[cols].isna().sum().sum()
    return AssetCheckResult(
        passed=bool(nulos == 0),
        metadata={
            "filas_afectadas": MetadataValue.int(int(nulos))
        }
    )



# CHECKS DE TRANSFORMACIÓN ========================================
@asset_check(asset=assets_file.islas_agregadas_data)
def check_n_islas(islas_agregadas_data):
    n_islas = islas_agregadas_data['TERRITORIO#es'].nunique()
    return AssetCheckResult(
        passed=bool(n_islas == 7),
        metadata={
            "n_islas_detectadas": MetadataValue.int(int(n_islas))
        }
    )

@asset_check(asset=assets_file.datos_evolucion_temporal)
def check_continuidad_temporal(datos_evolucion_temporal):
    años_esperados = set(range(2015, 2024))
    años_presentes = set(datos_evolucion_temporal['Año'].unique())
    faltantes = sorted(años_esperados - años_presentes)
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        metadata={
            "años_faltantes": MetadataValue.text(str(faltantes)),
            "rango_temporal": MetadataValue.text(f"{min(años_presentes)}-{max(años_presentes)}")
        }
    )

@asset_check(asset=assets_file.datos_heatmap_tenerife)
def check_municipios_tenerife(datos_heatmap_tenerife):
    n_mun = datos_heatmap_tenerife['NOMBRE'].nunique()
    return AssetCheckResult(
        passed=bool(n_mun == 31),
        metadata={
            "n_municipios": MetadataValue.int(int(n_mun))
        }
    )





# CHECKS DE VISUALIZACIÓN ========================================
@asset_check(asset=assets_file.viz_estructura_isla_2023)
def check_archivo_png_generado(viz_estructura_isla_2023):
    filepath = viz_estructura_isla_2023
    existe = os.path.exists(filepath)
    tamano = os.path.getsize(filepath) if existe else 0
    return AssetCheckResult(
        passed=bool(existe and tamano > 0),
        metadata={
            "archivo": MetadataValue.text(str(filepath)),
            "existe": MetadataValue.text(str(existe)),
            "tamano_bytes": MetadataValue.int(int(tamano))
        }
    )