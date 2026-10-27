FROM python:3.13-slim

WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.2.15

COPY generador.py producer.py ./

CMD ["python", "-u", "producer.py"]
