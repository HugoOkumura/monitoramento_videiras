FROM python:3.12-alpine3.23

WORKDIR /usr/src/app
COPY requirements.txt ./

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "./ingestor.py"]