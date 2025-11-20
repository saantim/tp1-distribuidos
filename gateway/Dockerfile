FROM python:3.13-slim
COPY shared /shared
COPY gateway /gateway
COPY requirements.txt /
COPY compose_config.yaml /gateway/

RUN pip install -r requirements.txt

WORKDIR /gateway
ENV PYTHONPATH=/
ENTRYPOINT ["/bin/sh"]
