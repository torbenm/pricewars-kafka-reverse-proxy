from kafka import KafkaConsumer
from kafka import TopicPartition
import threading

from flask import Flask, render_template
from flask_cors import CORS
from flask_socketio import SocketIO
from flask_socketio import send, emit

import json
import time

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, logger=True, engineio_logger=True)

kafka_endpoint = 'vm-mpws2016hp1-05.eaalab.hpi.uni-potsdam.de'

class KafkaHandler(object):
    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers = kafka_endpoint + ':9092')
        self.dumps = {}

        topics = ['buyOffer', 'revenue']
        for topic in topics:
            self.dumps[topic] = []
        self.consumer.assign([TopicPartition(topic, 0) for topic in topics])
        self.consumer.seek_to_beginning()

        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True  # Demonize thread
        self.thread.start()  # Start the execution

    def run(self):
        count = 0
        for msg in self.consumer:
            print('message', count)
            count += 1
            try:
                msg_json = json.loads(msg.value.decode('utf-8'))
                output_json = json.dumps({
                    "topic": msg.topic,
                    "timestamp": msg.timestamp,
                    "value": msg_json
                })
                self.dumps[str(msg.topic)].append(output_json)
                socketio.emit([str(msg.topic), output_json, namespace='/')
            except Exception as e:
                print('emit error', e)
                break
        self.consumer.close

kafka = KafkaHandler()


@app.route("/log/sales")
def getAll():
    return(json.dumps(kafka.dumps['buyOffer']))

@socketio.on('connect', namespace='/')
def test_connect():
    print('test_connect')
    emit('test', {})

@socketio.on('buyOffer', namespace='/')
def buy_offer_listener():
    print('buy_offer_listener')
    emit('test', {})

@app.route("/log/buyOffer")
def buyOffer():
    kafka_endpoint = 'vm-mpws2016hp1-05.eaalab.hpi.uni-potsdam.de'
    consumer = KafkaConsumer(consumer_timeout_ms = 3000, bootstrap_servers = kafka_endpoint + ':9092')

    consumer.assign([TopicPartition('buyOffer', 0)])
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

@app.route("/log/salesPerMinutes")
def salesPerMinutes():
    kafka_endpoint = 'vm-mpws2016hp1-05.eaalab.hpi.uni-potsdam.de'
    consumer = KafkaConsumer(consumer_timeout_ms = 3000, bootstrap_servers = kafka_endpoint + ':9092')

    consumer.assign([TopicPartition('SalesPerMinutes', 0)])
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
    #app.run(port=8001)
    socketio.run(app, port=8001)
