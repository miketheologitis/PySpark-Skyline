#################################################### REQUIREMENTS ########################################################################

Spark: 3.3.0   (3.3.X is fine)
Kafka: kafka_2.12-3.2.1
Hadoop (YARN/HDFS): hadoop-3.3.4 (Not necessarily)
Python : Python 3.10.6 (Not necessarily)

No extra packages required for Python.


--------------------- Kafka - Spark Integration -------------------------------------

Very helpful: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

In any case, check your Kafka version and find the maven repository that we will pass
as package in the spark-submit later. For our case it is :

	org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0


################################################# SET-UP ##############################################################################

----------------------------------------  configurations.py --------------------------------------------------
 
1. Make sure <CHK_POINT_DIR_LOCAL_SKYLINES> , <CHK_POINT_DIR_GLOBAL_SKYLINE> are correct for your enviroment (we recommend HDFS path),
currently they lie in the filesystem (executors in cluster mode communicate with ssh)
2. Make sure <KAFKA_BOOTSTRAP_SERVER> is correct for your enviroment. Also make sure you have <KAFKA_INPUT_TOPIC>, <KAFKA_LOCAL_SKYLINES_TOPIC>
and <KAFKA_OUTPUT_TOPIC> topics in the Kafka setup.
3. If you want to run the streaming job, i.e., 'stream_job.py' make sure <LOCAL_SKYLINE_TRIGGER_TIME> and <GLOBAL_SKYLINE_TRIGGER_TIME> match
your requirements. This is tricky but leaving them as they are.


######################################### RUNNING INSTRUCTIONS #######################################################################

Let's first see 'batch_job.py' and 'stream_job.py' input parameters (I will use 'batch_job.py' - the same hold for 'stream_job.py'):

Usage: batch_job.py <QUERY> <ALGO_NAME> <PARAM>
	`<QUERY>` : As formally defined in the paper 'Skyline Operator' we have the query. It is of the form "SKYLINE OF x1 MIN, x2 MAX, ..., xd MIN".
		Obviously, we can use either MIN or MAX in any dimension (the previous query was just for reference). 
	<ALGO_NAME> : (String) is the algorithm name. We have three algorithms so <ALGO_NAME> is "MR_DIM", "MR_GRID" or "MR_ANGLE".
	<PARAM> : (Integer) Depending on the algorithm we give a different parameter concerning partitions (local skylines) => parallelism.
		MR_DIM : The simplest one, we compute <PARAM> number of local skylines. In other words, we parition the first dimension
			in <PARAM> disjoint partitions as the paper suggets.
		MR_GRID : Now, things get interesting. <PARAM> is the number of times we divide EACH dimension. Hence, we get <PARAM>^D
			partitions (where D is the dimension), but not quite... In MR_GRID we have dominated partitions that get thrown away.
			The total partitions/local-skylines we compute is exactly <PARAM>^D - (<PARAM> - 1)^D . Requires some thought to see
			why this is the case, there are in depth comments in the code.
		MR_ANGLE : Here, <PARAM> is how many times we divide each angular coordinate dimension (there are D-1 angular coordinates) 
			hence we compute exactly <PARAM>^(D-1) local skylines.

Important: The only thing that is not supported is MAX queries when using MR_ANGLE (in any dimension). To implement them would require
tedious work with (negative/positive) radians. Also, there is the assumption that data lie in the FIRST octant, i.e., are positive (or zero).
All the algorithms work for any dimension and the partitioning was done exactly as the paper suggested (MR_ANGLE required some external research).

Tip: Use the formulas (for the amount of local skylines) with your available resources (cores/executors) to find the optimal <PARAM> for each
algorithm. For MR_GRID, MR_DIM we need to be certain that the driver can combine the amount of local skylines. If you use MR_ANGLE try to maximize parallelism
and do not think about the driver, the algorithm reduces the amount of points in each local skyline astonishingly much!

Examples:
	batch_job.py "SKYLINE OF x1 MIN, x2 MIN, x3 MIN" MR_ANGLE 12
	batch_job.py "SKYLINE OF x1 MAX, x2 MIN, x3 MAX, x4 MIN, x5 MAX" MR_GRID 2
	batch_job.py "SKYLINE OF x1 MAX, x2 MAX, x3 MAX, x4 MIN, x5 MAX, x6 MAX, x7 MAX" MR_DIM 12
	

-------------------------------------------- Batch job aka 'batch_job.py' --------------------------------------------------------------------

'batch_job.py' is created to run tests and time the algorithm. It is a streaming application (just like 'stream_job.py') but its job is to 
process all the data (when available) in <KAFKA_INPUT_TOPIC> (once), i.e., in a single batch. Then, again, in a single batch it process everything 
(when available) from <KAFKA_LOCAL_SKYLINES_TOPIC> and output the final skyline. This application is extremely useful because in the pure streaming
'stream_job.py' we cannot time things accurately and dig around to see how the algorithms compare.

Steps:

1. Make sure <KAFKA_INPUT_TOPIC>, <KAFKA_LOCAL_SKYLINES_TOPIC> and <KAFKA_OUTPUT_TOPIC> have no previous data in them since the batch
job will start with 'earliest' offsets and will read previous data. The best way to go about it is by deleting and recreating everything.
Then feeding the data into the input topic. We give the commands for clarity:
	1. a. ~$ kafka-topics.sh --bootstrap-server <KAFKA_BOOTSTRAP_SERVER> --topic <KAFKA_INPUT_TOPIC> --delete
	1. b. ~$ kafka-topics.sh --bootstrap-server <KAFKA_BOOTSTRAP_SERVER> --topic <KAFKA_LOCAL_SKYLINES_TOPIC> --delete
	1. c. ~$ kafka-topics.sh --bootstrap-server <KAFKA_BOOTSTRAP_SERVER> --topic <KAFKA_OUTPUT_TOPIC> --delete
	1. d. ~$ kafka-topics.sh --bootstrap-server <KAFKA_BOOTSTRAP_SERVER> --topic <KAFKA_INPUT_TOPIC> --create --partitions 5
	1. e. ~$ kafka-topics.sh --bootstrap-server <KAFKA_BOOTSTRAP_SERVER> --topic <KAFKA_LOCAL_SKYLINES_TOPIC> --create --partitions 5
	1. f. ~$ kafka-topics.sh --bootstrap-server <KAFKA_BOOTSTRAP_SERVER> --topic <KAFKA_OUTPUT_TOPIC> --create --partitions 1
	1. g. ~$ kafka-console-producer.sh --bootstrap-server <KAFKA_BOOTSTRAP_SERVER> --topic KAFKA_INPUT_TOPIC> < /<PATH>/points_D_2_N_100_000.csv
2. ~$ spark-submit (see later)

-------------------------------------------- Stream job aka 'stream_job.py' --------------------------------------------------------------------

The pure streaming application.

Steps:

1. First submit the application
	1.a. ~$ spark-submit (see later)
2. Send data to <KAFKA_INPUT_TOPIC>
	2.a. ~$ kafka-console-producer.sh --bootstrap-server <KAFKA_BOOTSTRAP_SERVER> --topic KAFKA_INPUT_TOPIC> < /<PATH>/points_D_2_N_100_000.csv

-------------------------------------------- spark-submit (same for 'stream_job.py', 'batch_job.py') -------------------------------------

I cannot provide steps on how to use spark-submit because the options are limitless and depend on your enviroment/requirements/needs. Please
run 'spark-submit' and see the options from Spark, they provide all the insight. I will give the Google-Cloud 'spark-submit' used in all my
tests. Keep in mind I had 3 Executors with 4 vCPU cores each and 8GB RAM each, and I run on single node cluster managed by 'YARN'. With all that in mind:

spark-submit \
	--master yarn \
	--num-executors 3 \
	--executor-cores 4 \
	--executor-memory 8g \
	--driver-memory 20g \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 \
	--py-files functions.py,configurations.py \
	batch_job.py "SKYLINE OF x1 MIN, x2 MIN, x3 MIN, x4 MIN, x5 MIN" MR_DIM 12

I also run (before I set up yarn) some tests in local mode. To do this:

spark-submit \
	--master local[*] \
	--driver-memory 48g \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 \
	--py-files functions.py,configurations.py \
	batch_job.py "SKYLINE OF x1 MIN, x2 MIN, x3 MIN, x4 MIN, x5 MIN" MR_DIM 12
	
Note: Notice '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1' and '--py-files functions.py,configurations.py'.

--------------------------------------------- !!! EXTREMELY IMPORTANT !!!! --------------------------------------------------------------------

If you want to rerun the application, follow the steps again but ALSO first delete directories <CHK_POINT_DIR_LOCAL_SKYLINES> ,
<CHK_POINT_DIR_GLOBAL_SKYLINE> as they hold the stream state-store. Streaming applications are not meant to be stopped and we have to manually
delete these directores (for both 'stream_job.py' and 'batch_job.py') in order to rerun them.
	1. ~$ rm -r <CHK_POINT_DIR_LOCAL_SKYLINES>    (if they are in HDFS then: ~$ hadoop fs -rm -rf <CHK_POINT_DIR_LOCAL_SKYLINES>)
	2. ~$ rm -r <CHK_POINT_DIR_GLOBAL_SKYLINES>   (if they are in HDFS then: ~$ hadoop fs -rm -rf <CHK_GLOBAL_DIR_LOCAL_SKYLINES>)









