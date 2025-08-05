FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY .env .

COPY ./app ./app

ENV PYTHONUNBUFFERED=1
ENV ENV_FILE=/app/.env

EXPOSE 80