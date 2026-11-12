FROM python:3.11-slim

WORKDIR /app

COPY app /app
COPY settings.json /app/settings.json
COPY checkpoint.json /app/checkpoint.json

RUN pip install fastapi uvicorn httpx

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
