FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY client.py .

RUN mkdir -p /files

ENTRYPOINT ["tail", "-f", "/dev/null"]