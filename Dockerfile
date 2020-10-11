FROM python:3.8-slim-buster

COPY . /src
WORKDIR /src

RUN python setup.py install

RUN pip install -r tests/requirements.txt

ENTRYPOINT ["./entrypoints.sh"]
