from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from config import BROKER_EXTERNAL_PORT, TOPIC

spark = SparkSession.builder.appName("KafkaStreamingDemo") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafka_broker = f"localhost:{BROKER_EXTERNAL_PORT}"

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_broker) \
  .option("startingOffsets", "earliest") \
  .option("kafka.security.protocol","SSL") \
  .option("subscribe", TOPIC) \
  .load()

# .option("kafka.ssl.protocol", "SSL") \
# .option("kafka.ssl.enabled.protocols", "SSL") \
# N.B. From the kafka topic, we have to read the data as a dataframe like dict. [CHALLANGE]
# We have to use - option("startingOffsets", "earliest") this, for 
# getting the historical data

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# df.selectExpr("CAST(value AS STRING)")

print(f"\n################################################################\n")
df.printSchema()
print(f"\n################################################################\n")

sample_schema = (
    StructType()
    .add("id", StringType())
    .add("first_name", StringType())
    .add("last_name", StringType())
)

# info_dataframe = df.select(
#     from_json(col("value"), sample_schema).alias("sample")
# )

query = df \
  .writeStream \
  .outputMode("append") \
  .format("console") \
  .start()
  
  # .outputMode("complete") \

print(f"\n################################################################\n")
print(f"\n\n ==> query: {query} \n")
query.explain()
print(query.status)
print(query.lastProgress)
print(f"\n################################################################\n")

query.awaitTermination()
# query.stop()