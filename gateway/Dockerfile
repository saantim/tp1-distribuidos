FROM python:3.13-slim
COPY shared /shared
COPY middleware /middleware
COPY gateway /gateway
WORKDIR /gateway
ENV PYTHONPATH=/
ENTRYPOINT ["/bin/sh"]
