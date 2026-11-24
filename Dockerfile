FROM apache/airflow:3.4.0-python3.12

# Install Playwright's Node driver runtime dependency.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libatomic1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Install uv (already included in the base image)
# RUN pip install uv

# Create requirements.txt based on pyproject.toml
COPY pyproject.toml ./
RUN uv pip compile pyproject.toml -o requirements.txt

# Append extensions' dependencies to requirements.txt
RUN echo "psycopg2-binary>=2.9.13" >> requirements.txt
RUN echo "apache-airflow-providers-slack>=9.11.1" >> requirements.txt
RUN echo "gspread>=6.2.1" >> requirements.txt
RUN echo "google-cloud-bigquery>=3.47.0" >> requirements.txt
RUN echo "pyarrow>=25.0.1" >> requirements.txt
RUN echo "astronomer-cosmos==1.16.0" >> requirements.txt
RUN echo "dbt-core==1.12.7" >> requirements.txt
RUN echo "dbt-bigquery==1.12.4" >> requirements.txt
RUN echo "dbt-postgres==1.11.0" >> requirements.txt
RUN echo "playwright==1.65.1" >> requirements.txt

# Install dependencies based on the requirements.txt file
RUN pip install -r requirements.txt

# Ensure Python processes pick up UTF-8 locale
ENV LANGUAGE=ko_KR.UTF-8
ENV LANG=ko_KR.UTF-8
ENV LC_ALL=ko_KR.UTF-8
