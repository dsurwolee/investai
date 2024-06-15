FROM python:3.9-slim

WORKDIR /app

RUN mkdir -p data

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "transform_to_ohlc.py"]
