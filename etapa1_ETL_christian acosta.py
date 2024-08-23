# -*- coding: utf-8 -*-
"""
Created on Wed May 29 11:14:04 2024

@author: Tj - Christian
"""


import requests
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
import logging
from datetime import datetime, timedelta
from tqdm import tqdm  # Importamos tqdm para la barra de progreso

# Configuración de logging
logging.basicConfig(filename='etl_process.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# URL de Wikipedia
WIKI_URL = "https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500"

def extract_wikipedia_sp500(url):
    try:
        response = requests.get(url)  # Realiza una solicitud HTTP GET a la URL dada.
        response.raise_for_status()  # Lanza una excepción si la respuesta HTTP tiene un error.
        soup = BeautifulSoup(response.content, 'html.parser')  # Analiza el contenido HTML de la respuesta.
        table = soup.find('table', {'class': 'wikitable sortable'})  # Encuentra la tabla con clase 'wikitable sortable'.
        df = pd.read_html(str(table))[0]  # Lee la tabla HTML en un DataFrame de pandas.
        logging.info("Extracción de datos de Wikipedia completada.")  # Registra un mensaje indicando que la extracción fue exitosa.
        print("Columnas extraídas:", df.columns)  # Imprime las columnas del DataFrame.
        return df  # Retorna el DataFrame.
    except Exception as e:
        logging.error(f"Error durante la extracción de Wikipedia: {e}")  # Registra un mensaje de error.
        raise  # Relanza la excepción.

def extract_yahoo_finance_data(ticker, start_date, end_date):
    try:
        stock_data = yf.download(ticker, start=start_date, end=end_date)  # Descarga datos históricos del ticker dado.
        if stock_data.empty:
            raise ValueError(f"No data found for {ticker}")  # Lanza una excepción si no se encontraron datos.
        stock_data['Ticker'] = ticker  # Añade una columna 'Ticker' con el valor del ticker.
        logging.info(f"Datos de {ticker} extraídos correctamente desde Yahoo Finance.")  # Registra un mensaje indicando que la extracción fue exitosa.
        return stock_data  # Retorna el DataFrame de datos históricos.
    except Exception as e:
        logging.error(f"Error durante la extracción de datos de {ticker} desde Yahoo Finance: {e}")  # Registra un mensaje de error.
        return None  # Retorna None en caso de error.

def transform_sp500_companies(df):
    try:
        df = df[['Símbolo', 'Seguridad', 'Sector GICS', 'Sub-industria GICS']]  # Selecciona las columnas relevantes.
        df.columns = ['Ticker', 'Company', 'Sector', 'Subsector']  # Renombra las columnas.
        df = df.dropna()  # Elimina filas con valores nulos.
        logging.info("Transformación de datos de empresas S&P 500 completada.")  # Registra un mensaje indicando que la transformación fue exitosa.
        return df  # Retorna el DataFrame transformado.
    except Exception as e:
        logging.error(f"Error durante la transformación de datos de empresas S&P 500: {e}")  # Registra un mensaje de error.
        raise  # Relanza la excepción.

def transform_stock_data(df):
    try:
        df = df.reset_index()  # Resetea el índice del DataFrame.
        df = df[['Date', 'Ticker', 'Close']]  # Selecciona las columnas relevantes.
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')  # Convierte la columna 'Date' al formato 'YYYY-MM-DD'.
        logging.info("Transformación de datos de precios de acciones completada.")  # Registra un mensaje indicando que la transformación fue exitosa.
        return df  # Retorna el DataFrame transformado.
    except Exception as e:
        logging.error(f"Error durante la transformación de datos de precios de acciones: {e}")  # Registra un mensaje de error.
        raise  # Relanza la excepción.

def load_data_to_csv(df, filename):
    try:
        df.to_csv(filename, index=False)  # Guarda el DataFrame en un archivo CSV sin el índice.
        logging.info(f"Datos guardados en {filename} correctamente.")  # Registra un mensaje indicando que la carga fue exitosa.
    except Exception as e:
        logging.error(f"Error al guardar los datos en {filename}: {e}")  # Registra un mensaje de error.
        raise  # Relanza la excepción.

def etl_process():
    try:
        # Fase de extracción
        sp500_df = extract_wikipedia_sp500(WIKI_URL)  # Extrae datos de la URL de Wikipedia.
        #print("Primeras 10 filas extraídas:\n", sp500_df.head(10))  # Imprime las primeras 10 filas del DataFrame extraído.

        # Fase de transformación
        sp500_df2 = transform_sp500_companies(sp500_df)  # Transforma el DataFrame extraído.
        #print("Primeras 10 filas transformadas:\n", sp500_df2.head(10))  # Imprime las primeras 10 filas del DataFrame transformado.

        # Guardar la lista de empresas transformada
        load_data_to_csv(sp500_df2, 'sp500_companies.csv')  # Guarda el DataFrame transformado en un archivo CSV.

        # Definir el rango de fechas para los precios de las acciones
        end_date = datetime.today().strftime('%Y-%m-%d')  # Obtiene la fecha de hoy en formato 'YYYY-MM-DD'.
        start_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')  # Calcula la fecha de 90 días atrás.

        # Extraer y transformar datos de precios de acciones
        all_stock_data = []
        for ticker in tqdm(sp500_df2['Ticker'], desc="Descargando datos de Yahoo Finance"):  # Barra de progreso para la extracción de datos.
            stock_data = extract_yahoo_finance_data(ticker, start_date, end_date)  # Extrae datos históricos del ticker.
            if stock_data is not None:
                try:
                    stock_data = transform_stock_data(stock_data)  # Transforma el DataFrame de datos históricos.
                    all_stock_data.append(stock_data)  # Añade el DataFrame transformado a la lista.
                except Exception as transform_error:
                    logging.error(f"Error durante la transformación de datos de {ticker}: {transform_error}")  # Registra un mensaje de error.

        # Concatenar todos los datos de precios de acciones
        if all_stock_data:
            all_stock_data_df = pd.concat(all_stock_data, ignore_index=True)  # Concatena todos los DataFrames en uno solo.
            # Guardar los precios de las empresas transformados
            load_data_to_csv(all_stock_data_df, 'sp500_stock_prices.csv')  # Guarda el DataFrame concatenado en un archivo CSV.

        logging.info("Proceso ETL completado exitosamente.")  # Registra un mensaje indicando que el proceso ETL fue exitoso.

    except Exception as e:
        logging.error(f"Error en el proceso ETL: {e}")  # Registra un mensaje de error.
        raise  # Relanza la excepción.
    
etl_process()


