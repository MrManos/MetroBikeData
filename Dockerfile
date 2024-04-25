FROM ubuntu:20.04

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y python3 python3-pip

COPY requirements.txt .

RUN pip3 install -r requirements.txt

WORKDIR /code

COPY /src .
COPY /test .