# The checkpoint directories are EXTREMELY IMPORTANT. It's the location where the State Store is at, where
# the system will write all the checkpoint information. This should be a directory in an HDFS-compatible
# fault-tolerant file system. Especially in cluster enviroment the easiest approach is to give an 
# HDFS path. For standarlone mode this could be a file in the system.
CHK_POINT_DIR_LOCAL_SKYLINES = "file:///home/hadoop/chk-point-dir" 
CHK_POINT_DIR_GLOBAL_SKYLINE = "file:///home/hadoop/chk-point-dir2"


# Kafka Configurations
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_INPUT_TOPIC = "input_skyline_topic"
KAFKA_LOCAL_SKYLINES_TOPIC = "local_skyline_topic"
KAFKA_OUTPUT_TOPIC = "output_skyline_topic"


# Maximum and Minimum value of data points
MINIMUM_VALUE = 0
MAXIMUM_VALUE = 1_000_000_000


# ! Only for 'project.py' where we are streaming. !
# The queries will be executed with micro-batches mode, where micro-batches will be kicked off at these specified intervals.
# 	1. If the previous micro-batch completes within the interval, then the engine will wait until the interval is over before
#      kicking off the next micro-batch.
#   2. If the previous micro-batch takes longer than the interval to complete (i.e. if an interval boundary is missed), 
#      then the next micro-batch will start as soon as the previous one completes (i.e., it will not wait for the next interval boundary).
#   3. If no new data is available, then no micro-batch will be kicked off.
# Important to note about micro-batches and intervals: The micro-batch size is determined by the rate at which new data is arriving 
# in the input source. In each micro-batch, Spark Structured Streaming will try to process all the available data that has accumulated since
# the last micro-batch was processed. If the input source is producing data at a high rate, then the size of the micro-batch will be large.
# If the input source is producing data at a slow rate, then the size of the micro-batch will be small.
LOCAL_SKYLINE_TRIGGER_TIME = "30 seconds"
GLOBAL_SKYLINE_TRIGGER_TIME = "20 seconds"

