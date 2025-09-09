FROM python:3.11-slim

WORKDIR /app


# Copia o requirements.txt primeiro para aproveitar cache do Docker
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt


# Garante que o diretório data exista
RUN mkdir -p /app/data

# Depois copia o código da aplicação
COPY app/ /app/

EXPOSE 8084

CMD ["python", "app.py"]
