FROM python:3.13-slim

COPY requirements.txt /
RUN pip install -r requirements.txt

COPY shared /shared
COPY gateway /gateway
COPY compose_config.yaml /gateway/

WORKDIR /gateway
ENV PYTHONPATH=/
ENTRYPOINT ["/bin/sh"]
