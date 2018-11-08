import argparse
import atexit
import json
import logging
import happybase

from kafka import KafkaConsumer

logger_format = "%(asctime)s - %(message)s"  #set logging format
logging.basicConfig(format = logger_format)
logger = logging.getLogger('data-storage-writer')  #save all log at 'data-producer'
logger.setLevel(logging.DEBUG)

def shutdown_hook(kafka_consumer, hbase_connection):
	"""
	shutdown hook before system down
	"""
	try:
		kafka_consumer.close()
		hbase_connection.close()
	except Exception as e:
		logger.warn('Failed to close kafka consumer or hbase connection for %s', e)



#Row Key Strategy 
# rowkey:symbol + timestamp    family:value 
# BTC-USD:timestamp1			6000
# BTC-USD:timestamp2			6001
# BTC-USD:timestamp3			6002

def persist_data(data, hbase_connection, data_table):
	"""
	persist data into hbase.

	this is how data_producer produced to kafka
	payload = {
		'Symbol': str(symbol),
		'LastTradePrice' : str(price),
		'Timestamp' : str(timestamp)
		}
	"""
	try:
		logger.debug('Start to persist data to hbase: %s', data)
		parsed = json.loads(data)
		symbol = parsed.get('Symbol')
		price = parsed.get('LastTradePrice')
		timestamp = parsed.get('Timestamp')

		table = hbase_connection.table(data_table)
		row_key = "%s-%s" % (symbol, timestamp)
		table.put(row_key, {
			'family:symbol': symbol,
			'family:trade_time': timestamp,
			'family:trade_price': price })

		logger.info('Presisend data tp hbase for symbol: %s, price: %s, timestamp: %s', symbol, price, timestamp)

	except Exception as e:
		logger.error('Failed to presist data to hbase for %s', e)




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
	kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

	#Initiate a hbase connection
	hbase_connection = happybase.Connection(hbase_host)			#hbase_host from parameter transfer

	# print(str(hbase_connection))
	# print hbase_connection.tables()

	#create a tavle in hbase if not exists
	hbase_tables=[table.decode() for table in hbase_connection.tables()]
	if data_table not in hbase_tables:
		hbase_connection.create_table(data_table, {'family': dict() })

	#setup shutdown hook.
	atexit.register(shutdown_hook, kafka_consumer, hbase_connection)

	#start consuming from kafka and write to hbase
	for msg in kafka_consumer:
		persist_data(msg.value, hbase_connection, data_table )






# docker run -d -h myhbase -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16201:16201 -p 16301:16301 --name hbase harisekhon/hbase

# docker run -d -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=localhost -e KAFKA_ADVERTISED_PORT=9092 --link hbase:zookeeper confluent/kafka