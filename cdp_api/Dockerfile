FROM python:3

RUN apt-get -y update

WORKDIR /code

ADD requirements.txt /code
RUN pip install -r requirements.txt

ADD . /code

CMD python run_app.py
