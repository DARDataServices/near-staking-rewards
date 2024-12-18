FROM python:3.11-slim

WORKDIR /app

ADD . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "./collector.py"]