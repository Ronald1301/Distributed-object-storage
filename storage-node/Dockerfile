FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# El volumen se inicializa con este directorio (propietario: usuario sin privilegios)
RUN useradd --uid 1000 --no-create-home app && mkdir -p /data && chown app:app /data
USER app
VOLUME ["/data"]

EXPOSE 9000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000", "--no-access-log"]
