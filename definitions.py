from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
import assets_file
import checks_files

defs = Definitions(
    assets=load_assets_from_modules([assets_file]),
    asset_checks=load_asset_checks_from_modules([checks_files])
)