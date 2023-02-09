import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
# Create a Spark session
spark = SparkSession.builder.appName("ETL").getOrCreate()

df = spark.read.csv("yellow_trip_data_nov_22.csv", header=True, inferSchema=True)
vendor = spark.read.csv("vendor.csv", header=True, inferSchema=True,)
payment_type = spark.read.csv("payment_type.csv", header=True, inferSchema=True,)
rate = spark.read.csv("rate.csv", header=True, inferSchema=True,)


# duration
df2=df.withColumn('tpep_pickup_datetime',to_timestamp(col('tpep_pickup_datetime')))\
  .withColumn('tpep_dropoff_datetime', to_timestamp(col('tpep_dropoff_datetime')))\
  .withColumn('duration_sec',col("tpep_dropoff_datetime").cast("long") - col('tpep_pickup_datetime').cast("long")).select('tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration_sec').distinct()

window = Window.orderBy(col('tpep_pickup_datetime'))
duration = df2.withColumn('id', row_number().over(window))
duration = duration.select('id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration_sec')


# location
location = spark.read.csv('taxi+_zone_lookup.csv', header=True)
location = location.withColumnRenamed('LocationID', 'location_id')


#trip
df2=df.select('PULocationID', 'DOLocationID').distinct().withColumnRenamed('PULocationID', 'pick_up_location_id').withColumnRenamed('DOLocationID', 'dropoff_location_id')
window = Window.orderBy(col('pick_up_location_id'))
trip = df2.withColumn('id', row_number().over(window))
trip = trip.select('id', 'pick_up_location_id', 'dropoff_location_id')



duration_join = df.join(duration, (df.tpep_pickup_datetime == duration.tpep_pickup_datetime) & (df.tpep_dropoff_datetime == duration.tpep_dropoff_datetime)).withColumnRenamed('id', 'datetime_id')

payment_type_join = duration_join.join(payment_type, payment_type.payment_type_code == duration_join.payment_type).withColumnRenamed('payment_type_code', 'payment_type_id')

trip_join = payment_type_join.join(trip, (trip.pick_up_location_id == payment_type_join.PULocationID) & (trip.dropoff_location_id == payment_type_join.DOLocationID)).withColumnRenamed('id', 'trip_id')


window = Window.orderBy(col('datetime_id'))
fact_table = trip_join.withColumn('id', row_number().over(window))
fact_table = fact_table.select('id', 'payment_type_id', 'datetime_id', 'passenger_count', 'trip_distance', 'RatecodeID', 'VendorID', 'trip_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee', 'store_and_fwd_flag')



df_list = [[vendor, "dbo.Dim_Vendor"], [payment_type, "dbo.Dime_Payment_type"], [rate, "dbo.Dim_Rate"], [duration, "dbo.Dim_dt"], [location, "dbo.Dim_location"] , [trip, "dbo.Dim_trip"], [fact_table, "dbo.fact_table"]]

for df_item in df_list:
    df_item[0].write.format("jdbc").options(url="jdbc:sqlserver://localhost:1433;databaseName=nyc;user=test;password=test",
    driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
    dbtable=df_item[1],
    ).mode("overwrite").save()


spark.stop()