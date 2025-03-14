# Weather-ETL-Pipeline-with-Airflow-Postgres-PowerBI-Docker

### Overview
This project implements an **ETL (Extract, Transform, Load) pipeline** using Apache Airflow to fetch weather data for Vietnamese provinces from the Open Meteo API, transform it into a structured format, and load it into a PostgreSQL database managed via DBeaver. The data is then visualized in a **Power BI dashboard** to provide insights into weather patterns, including temperature, humidity, precipitation, wind speed, and more. DBeaver is integrated as a SQL client for querying and managing the PostgreSQL database, enhancing data exploration and maintenance.

### Key Components
1. **Airflow DAG**: Automates the ETL process to fetch hourly weather data for Vietnamese provinces.
2. **PostgreSQL Database**: Stores the transformed weather data, managed and queried using DBeaver.
3. **DBeaver**: A SQL client tool for database administration, query execution, and data visualization, connect via localhost and container.
4. **Power BI Dashboard**: Visualizes the weather data with interactive charts, including a dual-axis chart for humidity and precipitation, time series charts, and maps.
