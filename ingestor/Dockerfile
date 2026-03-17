FROM python:3.12-slim

WORKDIR /usr/src/app

COPY ../models/ .
COPY ingestor/requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . ./

CMD ["python", "ingestor/ingestor.py"]