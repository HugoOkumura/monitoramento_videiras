FROM python:3.12-slim

WORKDIR /usr/src/app

COPY models/ /usr/src/app/
COPY pre_processor/requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY pre_processor/ ./

CMD ["python", "preprocessor.py"]

