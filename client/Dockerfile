FROM python:3.13-slim
COPY shared /shared
COPY client /client
COPY compose_config.yaml /client/
COPY requirements.txt /

RUN pip install -r requirements.txt

WORKDIR /client
ENV PYTHONPATH=/
ENTRYPOINT ["/bin/sh"]
