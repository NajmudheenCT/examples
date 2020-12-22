import json
import time

from kafka import KafkaConsumer
import pandas as pd


class DataReceiver(object):
    def __init__(self, name):
        super(DataReceiver, self).__init__()
        self._name = name

    def run(self):
        raise NotImplemented


class KafkaDataReceiver(DataReceiver):
    def __init__(self):
        super(KafkaDataReceiver, self).__init__(name="kafka")

    def consume(self):
        consumer = KafkaConsumer('delfin-kafka',
                                 bootstrap_servers='localhost:9092')
        ts = []
        val =[]
        for msg in consumer:
            perf = msg.value
            r = json.loads(perf.decode())
            for m in r:
                metric = m[0]
                if 'bandwidth' == metric:
                    for key in m[2]:
                        ts.append(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(key))))
                        val.append(m[2][key])

            time.sleep(20)
            print('Iam here')
            csv = {'timestamp': ts, 'value': val}
            df = pd.DataFrame(csv)

            # saving the dataframe
            df.to_csv('file3.csv', header=True, index=False)




while (True):
    recv = KafkaDataReceiver()
    recv.consume()

