from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules, define_asset_job, \
    AssetSelection, sensor, RunRequest
import assets_file
import checks_files
import os

pipeline_job = define_asset_job(
    name="pipeline_completo_job",
    selection=AssetSelection.all()
)


@sensor(job=pipeline_job)
def sensor_datos(context):
    ficheros = [
        'distribucion-renta-canarias.csv',
        'codislas.csv',
        'nivelestudios.xlsx'
    ]

    last_mtime = context.cursor or "0"
    max_mtime = last_mtime

    for fichero in ficheros:
        if os.path.exists(fichero):
            curr_mtime = str(os.path.getmtime(fichero))
            if curr_mtime > max_mtime:
                max_mtime = curr_mtime

    if max_mtime != last_mtime:
        context.update_cursor(max_mtime)
        yield RunRequest(run_key=max_mtime)


defs = Definitions(
    assets=load_assets_from_modules([assets_file]),
    asset_checks=load_asset_checks_from_modules([checks_files]),
    jobs=[pipeline_job],
    sensors=[sensor_datos]
)