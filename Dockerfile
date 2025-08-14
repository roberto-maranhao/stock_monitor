FROM python:3.11-slim

WORKDIR /app


# Copia o requirements.txt primeiro para aproveitar cache do Docker
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Depois copia o código da aplicação
COPY app/ /app/

EXPOSE 8085

CMD ["python", "app.py"]
