from kafka import KafkaConsumer
from kafka import TopicPartition
import threading

from flask import Flask, render_template, request, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO
from flask_socketio import send, emit

import json
import time
import csv
import time

app = Flask(__name__, static_url_path='')
CORS(app)
socketio = SocketIO(app, logger=True, engineio_logger=True)

kafka_endpoint = 'vm-mpws2016hp1-05.eaalab.hpi.uni-potsdam.de'

'''
The following topics exist in kafka_endpoint:

  'deleteConsumer','getConsumers','getProducts','test','getMerchant','getMerchants','restockOffer','kumulativeRevenueBasedMarketshare',
  'kumulativeTurnoverBasedMarketshare','addConsumer','kumulativeAmountBasedMarketshare','sales','deleteOffer','marketshare','buyOffer',
  'addOffer','revenue','updates','__consumer_offsets','kumulativeTurnoverBasedMarketshareDaily','addProduct','kumulativeRevenueBasedMarketshareDaily',
  'getConsumer','getOffers','kumulativeAmountBasedMarketshareHourly','buyOffers','kumulativeRevenueBasedMarketshareHourly','deleteProduct',
  'getOffer','updateOffer','kumulativeTurnoverBasedMarketshareHourly','addMerchant','deleteMerchant','kumulativeAmountBasedMarketshareDaily',
  'producer','SalesPerMinutes','getProduct','salesPerMinutes'
'''
topics = ['buyOffer', 'revenue', 'updateOffer', 'updates', 'salesPerMinutes', 'kumulativeAmountBasedMarketshare', 'kumulativeTurnoverBasedMarketshare']


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
        self.consumer = KafkaConsumer(bootstrap_servers = kafka_endpoint + ':9092')
        self.dumps = {}
        end_offset = {}

        for topic in topics:
            self.dumps[topic] = []
            current_partition = TopicPartition(topic,0)
            self.consumer.assign([current_partition])
            self.consumer.seek_to_end()
            offset = self.consumer.position(current_partition)
            end_offset[topic] = offset>100 and offset or 100

        topicPartitions = [TopicPartition(topic, 0) for topic in topics]
        self.consumer.assign(topicPartitions)
        for topic in topics:
            self.consumer.seek(TopicPartition(topic,0),end_offset[topic]-100)

        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True  # Demonize thread
        self.thread.start()  # Start the execution

    def run(self):
        count = 0
        for msg in self.consumer:
            count += 1
            try:
                msg_json = json.loads(msg.value.decode('utf-8'))
                if 'http_code' in msg_json and msg_json['http_code'] != 200:
                    continue

                output_json = json.dumps({
                    "topic": msg.topic,
                    "timestamp": msg.timestamp,
                    "value": msg_json
                })
                self.dumps[str(msg.topic)].append(output_json)

                socketio.emit(str(msg.topic), output_json, namespace='/')
            except Exception as e:
                print('error emit msg', e)

        self.consumer.close

kafka = KafkaHandler()

@app.route("/export/data")
def export_csv():
    consumer2 = KafkaConsumer(consumer_timeout_ms = 3000, bootstrap_servers = kafka_endpoint + ':9092')

    topicPartitions = [TopicPartition(topic, 0) for topic in topics]
    consumer2.assign(topicPartitions)
    consumer2.seek_to_beginning()

    filename = int(time.time())
    filepath = 'data/'+str(filename)+'.csv'
    with open(filepath, 'wt', newline='') as csv_file:
        writer = csv.writer(csv_file)
        for msg in consumer2:
            msg2 = json.loads(msg.value.decode('utf-8'))
            writer.writerow([msg.topic, msg.timestamp, msg2])

    consumer2.close()
    return(json.dumps({"url":filepath}))

#@app.route('/dl/csv')
#def send_csv():
#    return send_from_directory('csv',"data/dump.csv")

@app.route('/csv/<path:path>')
def static_proxy(path):
  # send_static_file will guess the correct MIME type
  return app.send_static_file(path)

if __name__ == "__main__":
    #app.run(port=8001)
    socketio.run(app, port=8001)
