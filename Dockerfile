FROM python:3.5.2

ENV APP_HOME /loggerapp
RUN mkdir $APP_HOME
WORKDIR $APP_HOME

ADD . $APP_HOME

RUN pip install -r requirements.txt

CMD ["./wait-for-it.sh", "kafka:9092", "-t", "0", "--", "python", "-u", "LoggerApp.py", "--port", "8001"]
