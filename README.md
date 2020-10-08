# Spark Structured Streaming sample

### Building Jar
```
mvn clean package
``` 

This will create jar with dependencies.

### Running the jar in Standalone mode
```
bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.5 --class com.sample.StreamingSample --name test-app2 --deploy-mode client --master local[2] ../kafka-structured-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar application.properties
```