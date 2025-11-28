FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Append dependencies to requirements.txt
RUN echo "streamlit==1.51.0" >> requirements.txt
RUN echo "requests>=2.32.4" >> requirements.txt

# Install dependencies based on the requirements.txt file
RUN apt-get update
RUN pip install -r requirements.txt

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit app (bind to 0.0.0.0 for external access)
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]