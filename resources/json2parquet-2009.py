import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "nyc_taxi_experiments", table_name = "2009", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "nyc_taxi_experiments", table_name = "2009", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("vendor_id", "string", "vendor_id", "string"), ("pickup_datetime", "string", "pickup_datetime", "string"), ("dropoff_datetime", "string", "dropoff_datetime", "string"), ("passenger_count", "int", "passenger_count", "int"), ("trip_distance", "double", "trip_distance", "double"), ("pickup_longitude", "double", "pickup_longitude", "double"), ("pickup_latitude", "double", "pickup_latitude", "double"), ("rate_code", "string", "rate_code", "string"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("dropoff_longitude", "double", "dropoff_longitude", "double"), ("dropoff_latitude", "double", "dropoff_latitude", "double"), ("payment_type", "string", "payment_type", "string"), ("fare_amount", "double", "fare_amount", "double"), ("surcharge", "double", "surcharge", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("total_amount", "double", "total_amount", "double")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("vendor_id", "string", "vendor_id", "string"), ("pickup_datetime", "string", "pickup_datetime", "string"), ("dropoff_datetime", "string", "dropoff_datetime", "string"), ("passenger_count", "int", "passenger_count", "int"), ("trip_distance", "double", "trip_distance", "double"), ("pickup_longitude", "double", "pickup_longitude", "double"), ("pickup_latitude", "double", "pickup_latitude", "double"), ("rate_code", "string", "rate_code", "string"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("dropoff_longitude", "double", "dropoff_longitude", "double"), ("dropoff_latitude", "double", "dropoff_latitude", "double"), ("payment_type", "string", "payment_type", "string"), ("fare_amount", "double", "fare_amount", "double"), ("surcharge", "double", "surcharge", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("total_amount", "double", "total_amount", "double")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://de-bera-nyc-cab-trips/dl-parquet/2009"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://de-bera-nyc-cab-trips/dl-parquet/2009"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()