FROM python:3.7
# -alpine

COPY . /app

RUN chmod +x /app/wait-for-it.sh

WORKDIR /app

RUN pip install -r requirements.txt