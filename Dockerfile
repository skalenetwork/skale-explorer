FROM python:3.8-buster

RUN apt-get update
RUN apt-get install libpq-dev -y
RUN curl -fsSL https://get.docker.com -o get-docker.sh
RUN sh get-docker.sh

RUN curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" \
    -o /usr/local/bin/docker-compose && \
    chmod +x /usr/local/bin/docker-compose

COPY ./requirements.txt /skale-explorer/requirements.txt
WORKDIR /skale-explorer

RUN pip3 install -r requirements.txt
COPY . /skale-explorer
ENV PYTHONPATH="/skale-explorer"