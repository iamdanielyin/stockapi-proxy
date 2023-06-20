FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10-slim
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
COPY ./app /app