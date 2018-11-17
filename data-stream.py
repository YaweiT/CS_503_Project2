import argparse
import atexit
import logging
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format = logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

def shutdown_hook(kafka_producer):
	"""
	shutdown hook before system down
	"""
	try:
		logger.info('Flushing pending messages to kafka, timeout is set to 10s')
		kafka_producer.flush(10)
		logger.info('Finish flushing pending msg to kafka')

	except KafkaError as kafka_error:
		logger.warn('Failed to flush pending msg to kafka, cause by: %s', kafka_error.message)
	finally:
		try:
			logger.info('Closing kafka commection')
			kafka_producer.close(10)
		except Exception as e:
			logger.warn('Failed to close kafka connection, caused by: %s', e.message)


def process_stream(stream, kafka_producer, target_topic):
# todo
	def send_to_kafka(rdd):
		results = rdd.collect()
		for r in results:
			data = json.dumps({
				'Symbol': r[0],
				'Timestamp': time.time(),
				'Average': r[1]
				})
		try:
			logger.info("sending average price %s to kafka", data)
			kafka_producer.send(target_topic, value = data.encode('utf-8'))
		except KafkaError as error:
			logger.warn('Failed to send average price to kafka: ', error.message)

	def pair(data):
		record = json.loads(data)
		return record.get('Symbol'), (float(record.get('LastTradePrice')), 1)

	stream.map(pair).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda kv: (kv[0], kv[1][0]/kv[1][1])).foreachRDD(send_to_kafka)


if __name__ == '__main__':

	#Setup command line arguments.
	parser = argparse.ArgumentParser()
	parser.add_argument('source_topic')
	parser.add_argument('target_topic')
	parser.add_argument('kafka_broker')
	parser.add_argument('batch_duration', help='the batch duration in secs')

	#Parse arguments
	args = parser.parse_args()
	source_topic = args.source_topic
	target_topic = args.target_topic
	kafka_broker = args.kafka_broker
	batch_duration = int(args.batch_duration)

	#Create SparkContext sc,  and StreamingContext ssc
	sc = SparkContext("local[2]", "AveragePrice")
	sc.setLogLevel('INFO')
	ssc = StreamingContext(sc, batch_duration)

	#Instactiate a kafka stram for processing
	directKafkaStream = KafkaUtils.createDirectStream(ssc, [source_topic], {'metadata.broker.list' : kafka_broker})

	#Extract value from directKafkastream(key, value)
	stream = directKafkaStream.map(lambda msg:msg[1])

	#Instantiate a simple kafkaproducer
	kafka_producer = KafkaProducer(bootstrap_servers = kafka_broker)

	process_stream(stream, kafka_producer, target_topic)

	#setup shutdown hook
	atexit.register(shutdown_hook, kafka_producer)

	ssc.start()
	ssc.awaitTermination()
