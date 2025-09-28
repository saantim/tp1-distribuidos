FROM python:3.13-slim
COPY shared /shared
COPY worker /worker
COPY requirements.txt /
RUN pip install -r requirements.txt
ENV PYTHONPATH=/
ENTRYPOINT ["/bin/sh"]
