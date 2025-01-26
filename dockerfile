FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Installation du package en mode développement
RUN pip install -e .

ENV PYTHONPATH=/app

CMD ["python", "src/main.py"]