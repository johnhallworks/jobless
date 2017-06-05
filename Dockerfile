FROM python:3.5-alpine
ADD . /opt/jobless
WORKDIR /opt/jobless
RUN pip install /opt/jobless
