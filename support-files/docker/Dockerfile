FROM python:3.10
WORKDIR /apps

RUN apt-get update -y
RUN apt-get install -y supervisor

ADD ./support-files/supervisor/app.conf  /etc/supervisor/conf.d/app.conf
ADD ./support-files/supervisor/celery.conf  /etc/supervisor/conf.d/celery.conf
ADD ./support-files/supervisor/beat.conf  /etc/supervisor/conf.d/beat.conf

ADD . .

RUN pip install -r ./requirements/requirements.txt
RUN pip install -r ./requirements/requirements-dev.txt
RUN pip install -r ./requirements/requirements-ops.txt

ENV SECRET_KEY django-insecure-wi*fg%43$^^bwbus!1+p2sud3wnpe@3$is$3h8$cdw6h+s!-ug
ENV APP_CODE weops-next
ENV DEBUG False
ENV DB_ENGINE  django.db.backends.sqlite3
ENV DB_NAME weops-next


CMD [ "supervisord","-n" ]