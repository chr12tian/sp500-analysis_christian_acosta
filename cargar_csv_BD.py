import pyodbc
import pandas as pd
from sqlalchemy import create_engine

# Configuración de la conexión
server = 'DESKTOP-TO6E6D6\\SQLEXPRESS01'
database = 'SP500'
username = 'root'
password = '12345'
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Conexión a la base de datos
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Crear el motor de SQLAlchemy
engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')

# Leer los archivos CSV
company_profiles_df = pd.read_csv(r'sp500_CompanyProfiles.csv')
companies_df = pd.read_csv(r'sp500_companies.csv')

# Eliminar filas con valores NULL en la columna 'Symbol'
company_profiles_df = company_profiles_df.dropna(subset=['Symbol'])

# Recortar los valores de 'FechaFundada' a una longitud máxima de 4 caracteres
company_profiles_df['FechaFundada'] = company_profiles_df['FechaFundada'].apply(lambda x: x[:4] if isinstance(x, str) else x)

# Eliminar filas con valores NULL en la columna 'Date'
companies_df = companies_df.dropna(subset=['Date'])

# Cargar los datos en las tablas
company_profiles_df.to_sql('CompanyProfiles', engine, if_exists='append', index=False)
companies_df.to_sql('Companies', engine, if_exists='append', index=False)

cursor.close()
conn.close()
