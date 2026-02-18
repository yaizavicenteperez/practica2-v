import pandas as pd

# DISTRIBUCION-RENTA-CANARIAS.CSV =======================================================
print('distribucion-renta-canarias.csv')
# Cargar el CSV
df = pd.read_csv('distribucion-renta-canarias.csv')

print("\n=== COLUMNAS ===")
print(df.columns.tolist())

print("\n=== INFO ===")
print(df.info())

print("\n=== VALORES ÚNICOS EN COLUMNAS PRINCIPALES ===")
for col in df.columns:
    print(f"\n{col}: {df[col].nunique()} valores únicos")
    if df[col].nunique() < 20:  # Si hay pocos valores, mostrarlos
        print(df[col].unique())


# CODISLAS.CSV =======================================================================
print('codislas.csv')
# Cargar con el encoding y separador correctos
cod = pd.read_csv('codislas.csv', encoding='latin-1', sep=';')

print("\n=== COLUMNAS ===")
print(cod.columns.tolist())

print("\n=== INFO ===")
print(cod.info())

print("\n=== Total registros ===")
print(f"Total: {len(cod)}")




# NIVELESTUDIOS.XLSX ======================================================

print("nivelesestudios.xlsx")

# Cargar el archivo Excel
df_estudios = pd.read_excel('nivelestudios.xlsx')

print("\n=== COLUMNAS ===")
print(df_estudios.columns.tolist())

print("\n=== INFO ===")
print(df_estudios.info())

print("\n=== DIMENSIONES ===")
print(f"Filas: {len(df_estudios)}, Columnas: {len(df_estudios.columns)}")

print("\n=== VALORES ÚNICOS POR COLUMNA ===")
for col in df_estudios.columns:
    n_unique = df_estudios[col].nunique()
    print(f"\n{col}: {n_unique} valores únicos")
    if n_unique < 20:  # Si hay pocos valores, mostrarlos
        print(f"  Valores: {df_estudios[col].unique()}")


