package com.sample;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class StreamingSample {

    private static final Logger logger = LoggerFactory.getLogger(StreamingSample.class);
    private static final String SOURCE_PROPS = "source.";
    private static final String DEST_PROPS = "dest.";

    public static void main(String[] args) throws StreamingQueryException {
        if (args.length < 1){
            logger.info("Unable to find configuration file. Please provide config file path.");
            return;
        }

        String filepath = args[0];
        System.out.println("Loading properties from: " + filepath);

        File tempFile = new File(filepath);
        if (!tempFile.exists()){
            logger.info("Config file provided does not exist.");
            return;
        }

        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(filepath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            logger.info("Error while reading config file path.");
            return;
        }

        Properties props = new Properties();
        try {
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("Error while reading config file path.");
            return;
        }

        SparkSession spark = SparkSession.builder().appName("Kafka Structured Streaming App").getOrCreate();

        String checkpointLocation = "checkpoints";
        if (props.containsKey("checkpointLocation"))
            checkpointLocation = props.getProperty("checkpointLocation");
        spark.conf().set("spark.sql.streaming.checkpointLocation", checkpointLocation);
        DataStreamReader dfStreamReader = spark
                .readStream()
                .format("kafka");

        Set<String> propertyNames = props.stringPropertyNames();
        for (String name : propertyNames ) {
            if (name.contains(SOURCE_PROPS)) {
                dfStreamReader.option(name.replace(SOURCE_PROPS, ""), props.getProperty(name));
                logger.info("Adding source property: {}, Value: {}", name.replace(SOURCE_PROPS, ""), props.getProperty(name) );
            }
        }
        Dataset<Row> df = dfStreamReader.load();

        ExpressionEncoder<Row> encoder = RowEncoder.apply(SampleEncoders.getEventSchemaEncoder());

        Dataset<Row> transformedFrame = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .flatMap(new FlatMapFunction<Row, Row>() {
                    @Override
                    public Iterator<Row> call(Row row) throws Exception {
                        List<JsonMsg> events = transform(row.getString(1));
                        List<Row> eventRows = new ArrayList<>();
                        for (JsonMsg msg : events) {
                            eventRows.add(RowFactory.create(msg.getMsg()));
                        }
                        return eventRows.iterator();
                    }
                }, encoder);

        DataStreamWriter<Row> ds = transformedFrame
                .selectExpr("CAST(value AS STRING)")
                .writeStream()
                .format("kafka");

        for (String name : propertyNames ) {
            if (name.contains(DEST_PROPS)) {
                ds.option(name.replace(DEST_PROPS, ""), props.getProperty(name));
                logger.info("Adding sink property: {}, Value: {}", name.replace(DEST_PROPS, ""), props.getProperty(name) );
            }
        }
        StreamingQuery ds1 =  ds.start();
        ds1.awaitTermination();
    }

    private static List<JsonMsg> transform(String string) {
        JsonMsg msg = new JsonMsg();
        msg.setMsg(string);
        List<JsonMsg> msgList = new ArrayList<>();
        msgList.add(msg);
        return msgList;
    }
}
