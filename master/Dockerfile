FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY master.py .

RUN mkdir -p /data

EXPOSE 5000

CMD ["python", "master.py"]