FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scraper.py .

# Persistent storage for the SQLite database
VOLUME ["/data"]

ENTRYPOINT ["python", "scraper.py"]
CMD []
