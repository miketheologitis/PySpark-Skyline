from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_csv, collect_list, to_json, cast, from_json
from pyspark.sql.types import IntegerType, BooleanType

# Mine
from functions import mr_dim_partition_key, create_all_schemas, find_skyline, mr_grid_partition_key, \
    best_mr_grid_partition_key, decimal_to_base, mr_grid_keep_partition, mr_angle_partition_key, handle_input
    
from configurations import *

from sys import argv, exit


if __name__ == "__main__":
    # better_functions : A list of length equal to the dimension 'd' of the points that has 'd' Python functions 
    #    (currently either 'min' or 'max'). If 'better_functions[i]' = min then we know that a point is better in the
    #    i-th dimension than another point if its x_i value is smaller (i.e. Query was "x_i MIN")
    # d : The dimension of the points
    # algo_name : the algorithm name 'MR_DIM' or 'MR_GRID' or 'MR_ANGLE'
    # algo_param : The partitioning parameter of each algorithm. For 'MR_DIM' this parameter is the number of total partitions.
    #   For 'MR_GRID' it is how many times we divide each dimension (For example if 2 then x1 is divided into two halves, 
    #   x2 into two halves , ... with total 2^d partitions), and excluding the dominated partitions we will compute local skylines
    #   for exactly ( (algo_param^d) - (algo_param-1)^d ) partitions. For 'MR_ANGLE' it is how many times we divide each angular coordinate
    #   dimension (Note: there are d-1 angular coordinates, thus for input 2 we have 2^(d-1) partitions)
    better_functions, d, algo_name, algo_param = handle_input(*argv[1:])

    if better_functions is None: exit()

    # The name of the pyspark-submit job that will show in history
    APP_NAME = f"Skyline. Algorithm : {algo_name} , Query : {argv[1]}, Dimension : {d}, Algorithm Parameter : {algo_param}, N = 100_000"

    # Helpful Schemas
    columns, csv_schema, skyline_schema = create_all_schemas(d)

    # UDF for computing local skyline
    # x1_column, x2_column, ... --> (x1_row1, x2_row1, ...), (x1_row2, x2_row2, ...), ...
    local_skyline_udf = udf(
        lambda *x: find_skyline(better_functions, list(zip(*x))),
        returnType=skyline_schema
    )

    # UDF for computing global skyline
    global_skyline_udf = udf(
        lambda all_local_skylines_points: find_skyline(better_functions, all_local_skylines_points),
        returnType=skyline_schema
    )

    if algo_name == 'MR_DIM':
        # UDF for finding the MR_DIM partitioning key (See comments in functions.py for mr_dim_partition_key)
        mr_dim_partition_key_udf = udf(
            lambda x: mr_dim_partition_key(algo_param, x),
            returnType=IntegerType()
        )

    if algo_name == 'MR_GRID':
        # UDF for finding the MR_GRID partitioning key (See comments in functions.py for mr_grid_partition_key)
        mr_grid_partition_key_udf = udf(
            lambda *x: mr_grid_partition_key(algo_param, *x),
            returnType=IntegerType()
        )

        # UDF for filtering out MR_GRID dominated tuples using their key. In order to understand this you must read comments in functions.py.
        # The following UDF is the heart of the MR_GRID algorithm for filtering out dominated partitions and you must first read about my
        # approach in comments. More specifically, first read 'mr_grid_partition_key' then 'mr_grid_partition_dominates_partition' and lastly
        # 'decimal_to_base' and 'mr_grid_keep_partition'.
        mr_grid_keep_tuple_udf = udf(
            lambda key: mr_grid_keep_partition(better_functions, best_mr_grid_partition_key(better_functions, algo_param), decimal_to_base(key, algo_param, d)),
            returnType=BooleanType()
        )

    if algo_name == 'MR_ANGLE':
        # UDF for finding the MR_ANGLE partitioning key (See comments in functions.py for mr_angle_partition_key)
        mr_angle_partition_key_udf = udf(
            lambda *x: mr_angle_partition_key(algo_param, *x),
            returnType=IntegerType()
        )

    spark = SparkSession \
        .builder \
        .appName(APP_NAME) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") # due to annoying Kafka warning for UDFs.

    # Streaming Dataframe with columns: "x1", ..., "x_d" , where each x_i is the coordinate value in i-th dimension
    input_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_csv("value", csv_schema).alias("data")) \
        .select("data.*")

    """ Step 1. Partitioning - Map Process """

    # Create the partitioning key (column) according to the algorithm (MR_DIM, MR_GRID, MR_ANG)
    if algo_name == "MR_DIM":
        # We always choose "x1" dimension to partition by.
        partitioned_df = input_df \
            .withColumn("key", mr_dim_partition_key_udf(col("x1")))

    if algo_name == "MR_GRID":
        # Create partitioning 'key' and also drop dominated partition. Please read 'mr_grid_partition_key'
        partitioned_df = input_df \
            .withColumn(
                "key",
                mr_grid_partition_key_udf(
                    *[col(column) for column in columns]
                )
            ).filter(mr_grid_keep_tuple_udf(col('key')))

    if algo_name == 'MR_ANGLE':
        # Create partitioning 'key' using MR_ANGLE. Please read 'mr_angle_partition_key'
        partitioned_df = input_df \
            .withColumn(
                "key",
                mr_angle_partition_key_udf(
                    *[col(column) for column in columns]
                )
            )

    """ Step 2. Local Skyline Computation: """

    # For each partition compute local Skyline. Note: Each partition only contains a distinct "key" (Step 1.)
    # local_skylines_df has columns: "key" (From Step 1.) , "local_skyline" (the local skyline as an Array of points)
    local_skylines_df = partitioned_df \
        .groupBy(col("key")) \
        .agg(
            local_skyline_udf(
                *[collect_list(column) for column in columns]
            ).alias("local_skyline")
        )

    # Prepare to write to Kafka each Local Skyline : ex. For partition A with "key" = 1 -> [{"x1":7,"x2":4},{"x1":4,"x2":6},{"x1":5,"x2":5}]
    kafka_target_local_skylines_df = local_skylines_df \
        .select(
            col("key").cast("string"), 
            to_json(col("local_skyline")).alias("value")
        )

    # Write to Kafka each UPDATED (modified from current Batch) Local Skyline.
    local_skylines_query_write = kafka_target_local_skylines_df \
        .writeStream \
        .trigger(once=True) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("topic", KAFKA_LOCAL_SKYLINES_TOPIC) \
        .outputMode("update") \
        .option("checkpointLocation", CHK_POINT_DIR_LOCAL_SKYLINES) \
        .queryName("Kafka Local Skyline Query") \
        .start()

    local_skylines_query_write.awaitTermination()

    # Streaming Dataframe from Kafka input with only column: "local_skyline" which corresponds to the local
    # skyline of some Partition with specific distinct partitioning "key" (See Step 1.)
    kafka_local_skylines_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_LOCAL_SKYLINES_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(
            from_json(
                col("value").cast("string"),
                schema=skyline_schema
            ).alias("local_skyline")
        )


    """ Step 3. Global Skyline Computation - Merge Local Skylines: """

    # Drop duplicate points from all local_skylines. To explain why we do this consider a partition A. The local_skyline of this partition
    # will be taken into account in the aggregation every time it changes. This means that even if that local_skyline changes by one point,
    # we will be forced to compare (merge) identical points again. Thus, we explode, and drop duplicate points. We end up with a Dataframe that
    # contains all the distinct local_skyline points from all partitions, one in each row. 
    distinct_global_skyline_points_df = kafka_local_skylines_df \
        .selectExpr("explode(local_skyline) as local_skyline_points")
        #.dropDuplicates()

    global_skyline_df = distinct_global_skyline_points_df \
        .agg(
            global_skyline_udf(
                collect_list(col("local_skyline_points"))
            ).alias("global_skyline")
        )

    kafka_target_global_skyline_df = global_skyline_df \
        .select(
            to_json(col("global_skyline")).alias("value")
        )

    # Write to Kafka each the Global Skyline.
    global_skyline_query_write = kafka_target_global_skyline_df \
        .writeStream \
        .trigger(once=True) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("topic", KAFKA_OUTPUT_TOPIC) \
        .outputMode("complete") \
        .option("checkpointLocation", CHK_POINT_DIR_GLOBAL_SKYLINE) \
        .queryName("Kafka Global Skyline Query") \
        .start()


    global_skyline_query_write.awaitTermination()
