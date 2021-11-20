FROM python:3-slim as base
COPY requirements.txt ./
RUN pip3 install --upgrade pip \
    && pip3 install -r requirements.txt \
    && rm requirements.txt



FROM base as development
RUN apt-get update -y \ 
    && apt-get install git gcc -y \
    && pip3 install pep8 pytest coverage

FROM base as deployment