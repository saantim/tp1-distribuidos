FROM python:3.13-slim

# Limpia cache de apt para reducir tamaño de imagen
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /
RUN pip install -r requirements.txt

COPY shared /shared
COPY health_checker /health_checker

WORKDIR /health_checker
ENV PYTHONPATH=/
ENTRYPOINT ["/bin/sh"]
