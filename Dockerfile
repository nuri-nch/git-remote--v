FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install pyspark pytest flake8

CMD ["python"]