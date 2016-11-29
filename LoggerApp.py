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
        self.consumer.assign([TopicPartition('buyOffer', 0)])
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
                msg_json = json.dumps(msg.value.decode('utf-8'))
                output_json = json.dumps({
                    "topic": msg.topic,
                    "timestamp": msg.timestamp,
                    "value": msg_json
                })
                socketio.emit('buyOffer', json.dumps({"topic": msg.topic,"timestamp": msg.timestamp,"value": msg.value.decode('utf-8')}), namespace='/test')
            except Exception as e:
                print('emit error', e)
                break
        self.consumer.close

kafka = KafkaHandler()


@app.route("/log/sales")
def getAll():
    print('received request')
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

@socketio.on('connect', namespace='/test')
def test_connect():
    print('test_connect')
    emit('test', {})

@socketio.on('buyOffer', namespace='/test')
def buy_offer_listener():
    print('buy_offer_listener')
    emit('buyOffer', 'buy_offer_listener')


if __name__ == "__main__":
    #app.run(port=8001)
    socketio.run(app, port=8001)
