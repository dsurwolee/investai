FROM python:3.9-slim

WORKDIR /app

RUN mkdir -p feature_store

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "load_feature_store.py"]
