# sp500-analysis_christian_acosta

Proyecto ETL Automatizado para Empresas del S&P 500
Descripción del Proyecto
Este proyecto implementa un proceso ETL (Extract, Transform, Load) automatizado para extraer la lista de empresas del S&P 500 desde Wikipedia, obtener información adicional y descargar los precios diarios de cotización del último trimestre. Los estudiantes deberán implementar funciones para las fases de extracción (EXTRACT), transformación (TRANSFORM) y carga (LOAD) de los datos.

Requisitos
Python 3.x
Pandas
Requests
BeautifulSoup4
SQLAlchemy
pyodbc
Power BI
Estructura del Proyecto
├── data/
│   ├── raw/
│   ├── processed/
├── notebooks/
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
├── README.md
├── requirements.txt

Instrucciones de Instalación y Uso
Clona el repositorio:
git clone https://github.com/tu_usuario/tu_repositorio.git
cd _repositorio

Crear un entorno virtual e instalar las dependencias:
python -m venv venv
source venv/bin/activate  # En Windows usa `venv\Scripts\activate`
pip install -r requirements.txt

Configurar entorno:
tener acceso a una instancia de SQL Server.
Configura las credenciales de acceso en un archivo .env.
Ejecutar el proyecto:
python scripts/extract.py
python scripts/transform.py
python scripts/load.py

Descripción de Cada Fase del Proyecto
Extracción de Datos de las Empresas del S&P 500
Objetivo: Obtener la lista de empresas del S&P 500 desde Wikipedia y sus precios diarios de cotización del último trimestre.
Herramientas: Requests, BeautifulSoup4
Análisis Estadístico Descriptivo e Inferencial
Objetivo: Realizar análisis estadísticos descriptivos e inferenciales sobre los datos obtenidos.
Herramientas: Pandas, NumPy
Almacenamiento de Datos en SQL Server
Objetivo: Cargar los datos transformados en una base de datos SQL Server.
Herramientas: SQLAlchemy, pyodbc
Creación del Dashboard en Power BI
Objetivo: Visualizar los datos mediante un dashboard interactivo en Power BI.
Herramientas: Power BI
Clusterización de las Acciones Según la Volatilidad
Objetivo: Agrupar las acciones en clusters basados en su volatilidad.
Herramientas: Scikit-learn
Publicación en GitHub
Objetivo: Publicar el proyecto y su documentación en GitHub para su revisión y uso por otros estudiantes.
