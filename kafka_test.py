from kafka import KafkaConsumer
from kafka import TopicPartition
from flask import Flask, render_template
import json

kafka_endpoint = 'vm-mpws2016hp1-05.eaalab.hpi.uni-potsdam.de'
consumer = KafkaConsumer(consumer_timeout_ms = 1000, bootstrap_servers = kafka_endpoint + ':9092')

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World"

@app.route("/log/sales")
def getAll():
    consumer.assign([TopicPartition('sales', 0)])
    consumer.seek_to_beginning()

    result = []

    for msg in consumer:
        try:
            msg2 = json.loads(msg.value.decode('utf-8'))
            result.append({"topic": msg.topic,"timestamp": msg.timestamp,"value": msg2})
        except:
            pass

    consumer.close()
    return(json.dumps(result))

if __name__ == "__main__":
    app.run(port=8001)
