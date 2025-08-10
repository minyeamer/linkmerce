FROM apache/airflow:3.0.3-python3.10

# Install uv (already included in the base image)
# RUN pip install uv

# Create requirements.txt based on pyproject.toml
COPY pyproject.toml ./
RUN uv pip compile pyproject.toml -o requirements.txt

# Append extensions' dependencies to requirements.txt
RUN echo "PyYAML>=6.0.2" >> requirements.txt
RUN echo "gspread>=6.2.1" >> requirements.txt
RUN echo "google-cloud-bigquery==3.35.0" >> requirements.txt
RUN echo "pyarrow>=21.0.0" >> requirements.txt

# Install dependencies based on the requirements.txt file
RUN pip install -r requirements.txt