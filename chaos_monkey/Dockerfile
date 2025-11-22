FROM python:3.13-slim
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*
COPY shared /shared
COPY chaos_monkey /chaos_monkey
COPY requirements.txt /

RUN pip install -r requirements.txt

WORKDIR /chaos_monkey
ENV PYTHONPATH=/
ENTRYPOINT ["/bin/sh"]
