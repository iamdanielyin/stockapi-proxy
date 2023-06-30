FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10-slim

LABEL maintainer="Daniel Yin <iamdanielyin@gmail.com>"

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ./app /app