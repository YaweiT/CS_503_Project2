import argparse
import atexit
import json
import happybase
import logging
import time

from kafka import KafkaProducer

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format = logger_format)
logger = logging.getLogger('data-storage-reader')
logger.setLevel(logging.DEBUG)

def shutdown_hook(kafka_producer, hbase_connection):
	"""
	shutdown hook before system down
	"""
	try:
		logger.info('Closing Kafka Producer')
		kafka_producer.flush(10)
		kafka_producer.close()
		logger.info('Kafka Producer Closed')

		logger.info('Closing HBase Connection')
		hbase_connection.close()
		logger.info('HBase Connection Closed')

	except Exception as e:
		logger.warn('Failed to close kafka Producer or hbase connection for %s', e)
	finally:
		logger.info('Existing program')


if __name__ == '__main__':

	#read all arguments from initialization command
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name')
	parser.add_argument('kafka_broker')
	parser.add_argument('data_table')
	parser.add_argument('hbase_host')

	#Parse arguments.
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	data_table = args.data_table
	hbase_host = args.hbase_host

	#Initiate a simplle kafka comsumer.
	kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

	#Initiate a hbase connection
	hbase_connection = happybase.Connection(hbase_host)			#hbase_host from parameter transfer

	#setup shutdown hook.
	atexit.register(shutdown_hook, kafka_producer, hbase_connection)

	#Exit if the table is not found.
	hbase_tables = [table.decode() for table in hbase_connection.tables()]
	if data_table not in hbase_tables:
		exit()


	#Scan table and push to kafka
	table  = hbase_connection.table(data_table)

	for key,data in table.scan():
		payload = {
			'Symbol' : data[b'family:symbol'].decode(),
			'LastTradePrice' : data[b'family:trade_price'].decode(),
			'Timestamp' : data[b'family:trade_time'].decode()
		}

		logger.debug('Read data from hbase: %s', payload)
		kafka_producer.send(topic=topic_name, value=json.dumps(payload).encode('utf-8'))

		time.sleep(1)
  