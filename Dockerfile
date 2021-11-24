FROM python:3-slim as base
COPY requirements.txt ./
RUN pip3 install --upgrade pip \
    && pip3 install -r requirements.txt \
    && rm requirements.txt

FROM base as development
RUN apt-get update -y \ 
    && apt-get install git gcc curl vim zip -y \
    && pip3 install pep8 pytest coverage \
    && curl -L https://dl.k8s.io/release/v1.19.7/bin/linux/amd64/kubectl -o /usr/bin/kubectl \
    && chmod +x /usr/bin/kubectl \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -r awscliv2.zip aws

ENV MODULE_NAME kalytical.facade

FROM base as deployment
COPY ./ /app
WORKDIR /app/src
#RUN coverage run -m pytest && coverage report -m --fail-under=80
CMD ["/bin/bash","-c", "python3 /app/src/kalytical/facade.py"]