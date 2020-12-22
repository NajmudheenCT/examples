from subprocess import call


def delete_kafka_topic(topic_name):
    call(["/opt/kafka/bin/kafka-topics.sh", "--zookeeper", "zookeeper-1:2181", "--delete", "--topic", topic_name])

delete_kafka_topic('dlefin-kafka')