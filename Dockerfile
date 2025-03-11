# Use the official Airflow image as the base
FROM apache/airflow:latest

# Set the working directory
WORKDIR /airflow

# Copy requirements.txt if additional Python packages are needed
COPY requirements.txt .

# Install dependencies 
RUN pip install --no-cache-dir -r requirements.txt

# Ensure DAGs directory exists before copying
COPY /airflow/dags /airflow/dags

# Set Airflow environment variables
ENV AIRFLOW_HOME=//airflow

# Initialize the Airflow database
RUN airflow db init

# Expose the Airflow webserver port
EXPOSE 8080

# Start the Airflow webserver and scheduler
CMD ["bash", "-c", "airflow scheduler & airflow webserver --port 8080"]