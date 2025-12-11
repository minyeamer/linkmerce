FROM apache/airflow:3.1.3-python3.12

# Set Korean locale to ensure proper handling of Korean text in HTTP responses
USER root
RUN locale-gen ko_KR.UTF-8
RUN localedef -f UTF-8 -i ko_KR ko_KR.UTF-8
USER airflow

# Install uv (already included in the base image)
# RUN pip install uv

# Create requirements.txt based on pyproject.toml
COPY pyproject.toml ./
RUN uv pip compile pyproject.toml -o requirements.txt

# Append extensions' dependencies to requirements.txt
RUN echo "apache-airflow-providers-slack==9.6.0" >> requirements.txt
RUN echo "gspread>=6.2.1" >> requirements.txt
RUN echo "google-cloud-bigquery==3.35.0" >> requirements.txt
RUN echo "pyarrow>=21.0.0" >> requirements.txt
RUN echo "playwright==1.56.0" >> requirements.txt

# Install dependencies based on the requirements.txt file
RUN pip install -r requirements.txt

# Ensure Python processes pick up UTF-8 locale
ENV LANGUAGE=ko_KR.UTF-8
ENV LANG=ko_KR.UTF-8
ENV LC_ALL=ko_KR.UTF-8