from kafka import KafkaConsumer
from kafka import TopicPartition
from flask import Flask, render_template
import socketio
import eventlet
import json
from flask_cors import CORS

sio = socketio.Server(cors_allowed_origins=['*:*'])
app = Flask(__name__)
CORS(app)

kafka_endpoint = 'vm-mpws2016hp1-05.eaalab.hpi.uni-potsdam.de'

@app.route("/")
def hello():
    return "Hello World"

@app.route("/log/sales")
def getAll():
    consumer = KafkaConsumer(consumer_timeout_ms = 3000, bootstrap_servers = kafka_endpoint + ':9092')

    consumer.assign([TopicPartition('byOffer', 0)])
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

@sio.on('byOffer', namespace='/socket.io')
def getAllSales(sid, environ):
    consumer = KafkaConsumer(bootstrap_servers = kafka_endpoint + ':9092')

    consumer.assign([TopicPartition('byOffer', 0)])
    consumer.seek_to_beginning()

    for msg in consumer:
        try:
            msg2 = json.loads(msg.value.decode('utf-8'))
            sio.emit(json.dumps({"topic": msg.topic,"timestamp": msg.timestamp,"value": msg2}), room=sid)
        except:
            pass
    #consumer.close()

if __name__ == "__main__":
    app.run(port=8001)

# wrap Flask application with engineio's middleware
app = socketio.Middleware(sio, app)
