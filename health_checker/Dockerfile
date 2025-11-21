FROM python:3.13-slim
COPY shared /shared
COPY health_checker /health_checker
COPY requirements.txt /

RUN pip install -r requirements.txt

WORKDIR /health_checker
ENV PYTHONPATH=/
ENTRYPOINT ["/bin/sh"]
