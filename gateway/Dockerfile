FROM python:3.13-slim
COPY shared /shared
COPY middleware /middleware
COPY gateway /gateway
COPY requirements.txt /

RUN pip install -r requirements.txt

WORKDIR /gateway
ENV PYTHONPATH=/
ENTRYPOINT ["/bin/sh"]
