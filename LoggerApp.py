import csv
import json
import threading
import time

import pandas as pd
from flask import Flask, send_from_directory, request, Response
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
from kafka import TopicPartition

from DataGeneration import create_marketsituation_csv

app = Flask(__name__, static_url_path='')
CORS(app)
socketio = SocketIO(app, logger=True, engineio_logger=True)

kafka_endpoint = 'vm-mpws2016hp1-05.eaalab.hpi.uni-potsdam.de'

'''
The following topics exist in kafka_endpoint:

  'deleteConsumer','getConsumers','getProducts','test','getMerchant','getMerchants','restockOffer',
  'addConsumer','sales','deleteOffer','marketshare','buyOffer',
  'addOffer','revenue','updates','__consumer_offsets','addProduct',
  'getConsumer','getOffers','buyOffers','deleteProduct',
  'getOffer','updateOffer','addMerchant','deleteMerchant',
  'producer','SalesPerMinutes','getProduct','salesPerMinutes'
'''
topics = ['buyOffer', 'revenue', 'updateOffer', 'updates', 'salesPerMinutes', 'cumulativeAmountBasedMarketshare', 'cumulativeTurnoverBasedMarketshare']

'''
kafka_producer.send(KafkaProducerRecord(
"updateOffer", s"""{
    "offer_id": $offer_id,
    "uid": ${offer.uid},
    "product_id": ${offer.product_id},
    "quality": ${offer.quality},
    "merchant_id": ${offer.merchant_id},
    "amount": ${offer.amount},
    "price": ${offer.price},
    "shipping_time_standard": ${offer.shipping_time.standard},
    "shipping_time_prime": ${offer.shipping_time.prime},
    "prime": ${offer.prime},
    "signature": "${offer.signature}",
    "http_code": 200,
    "timestamp": "${new DateTime()}"
}"""))

'''


class KafkaHandler(object):
    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers=kafka_endpoint + ':9092')
        self.dumps = {}
        end_offset = {}

        for topic in topics:
            self.dumps[topic] = []
            current_partition = TopicPartition(topic,0)
            self.consumer.assign([current_partition])
            self.consumer.seek_to_end()
            offset = self.consumer.position(current_partition)
            end_offset[topic] = offset>100 and offset or 100

        topic_partitions = [TopicPartition(topic, 0) for topic in topics]
        self.consumer.assign(topic_partitions)
        for topic in topics:
            self.consumer.seek(TopicPartition(topic, 0), end_offset[topic]-100)

        #self.thread = threading.Thread(target=self.run, args=())
        #self.thread.daemon = True  # Demonize thread
        #self.thread.start()  # Start the execution

    def run(self):
        count = 0
        for msg in self.consumer:
            count += 1
            try:
                msg_json = json.loads(msg.value.decode('utf-8'))
                if 'http_code' in msg_json and msg_json['http_code'] != 200:
                    continue

                output = {
                    "topic": msg.topic,
                    "timestamp": msg.timestamp,
                    "value": msg_json
                }
                output_json = json.dumps(output)
                self.dumps[str(msg.topic)].append(output)

                socketio.emit(str(msg.topic), output_json, namespace='/')
            except Exception as e:
                print('error emit msg', e)

        self.consumer.close

kafka = KafkaHandler()


@socketio.on('connect', namespace='/')
def on_connect():
    global kafka
    print('new client connected, try to transmit some history .. ')
    if kafka.dumps:
        for msg_topic in kafka.dumps:
            messages = kafka.dumps[msg_topic][-100:]
            print('topic:', msg_topic, len(messages), 'messages')
            emit(msg_topic, messages, namespace='/')

def json_response(obj):
    js = json.dumps(obj)
    resp = Response(js, status=200, mimetype='application/json')
    return resp

@app.route("/testdata")
def test_data():
    response = create_marketsituation_csv()
    return json_response(response)

@app.route("/topics")
def get_topics():
    return json.dumps(topics)

@app.route("/status")
def status():
    status_dict = {}
    for topic in kafka.dumps:
        status_dict[topic] = {
            'messages': len(kafka.dumps[topic]),
            'last_message': kafka.dumps[topic][-1] if kafka.dumps[topic] else ''
        }
    return json.dumps(status_dict)

@app.route("/export/data")
def export_csv():
    consumer2 = KafkaConsumer(consumer_timeout_ms=3000, bootstrap_servers=kafka_endpoint + ':9092')

    topic_partitions = [TopicPartition(topic, 0) for topic in topics]
    consumer2.assign(topic_partitions)
    consumer2.seek_to_beginning()

    filename = int(time.time())
    filepath = 'data/'+str(filename)+'.csv'
    with open(filepath, 'wt', newline='') as csv_file:
        writer = csv.writer(csv_file)
        for msg in consumer2:
            msg2 = json.loads(msg.value.decode('utf-8'))
            writer.writerow([msg.topic, msg.timestamp, msg2])

    consumer2.close()
    return json.dumps({"url": filepath})


@app.route("/export/data/<path:topic>")
def export_csv_for_topic(topic):
    response = {}

    try:
        if topic in topics:
            consumer = KafkaConsumer(consumer_timeout_ms=3000, bootstrap_servers=kafka_endpoint + ':9092')
            topic_partitions = [TopicPartition(topic, 0)]
            consumer.assign(topic_partitions)
            consumer.seek_to_beginning()

            filename = topic + '_' + str(int(time.time()))
            filepath = 'data/'+ filename +'.csv'

            msgs = []
            for msg in consumer:
                msg_json = json.loads(msg.value.decode('utf-8'))
                msgs.append(msg_json)

            df = pd.DataFrame(msgs)
            df.to_csv(filepath, index=False)
            response = {'url': filepath}
        else:
            response = {'error': 'unknown topic'}
    except Exception as e:
        response = {'error': 'failed with: ' + str(e)}

    consumer.close()
    return json.dumps(response)


@app.route('/data/<path:path>')
def static_proxy(path):
    return send_from_directory('data', path)

if __name__ == "__main__":
    #app.run(port=8001)
    socketio.run(app, port=8001)
