FROM quay.io/astronomer/astro-runtime:12.6.0

# Set working directory
WORKDIR /usr/local/airflow

# Copy requirements.txt and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs
COPY dags/ /usr/local/airflow/dags/
