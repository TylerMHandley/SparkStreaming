from pyspark import SparkContext

from pyspark.streaming import StreamingContext

from pyspark.stream.kafka import KafkaUtils


sc = SparkContext(appName='KafkaStreamingTest')
ssc = StreamingContext(sc, 60)
kafkaStream = KafkaUtils.createStream(ssc, 'kafka:9092', 'sparkInfo', {'info', '2'})
parsed = KafkaStream.map(lambda v: json.loads(v[1])
counted = parsed.count().map(lambda x:'Words in this batch: {}'.format(x))
topfive = counted.transform(lambda x: sc.parallelize(x.take(5)))
topfive.pprint()

