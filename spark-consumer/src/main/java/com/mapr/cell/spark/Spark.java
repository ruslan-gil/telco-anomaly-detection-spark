package com.mapr.cell.spark;


import com.mapr.cell.common.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;

import java.util.Set;

public class Spark {
    JavaStreamingContext jssc;

    public Spark(int duration) {
        SparkConf sparkConf = new SparkConf().setAppName("CDR-Analytics").setMaster("local[4]");
        jssc = new JavaStreamingContext(sparkConf, new Duration(duration));
    }
    

    public JavaPairInputDStream<String, String> getKafkaInputStream(Set<String> topics){
        return KafkaUtils.createDirectStream(jssc, String.class, String.class,
                        Config.getConfig().getPrefixedMap("kafka."),
                        topics);
    }

    public void start(){
        jssc.start();
        jssc.awaitTermination();
    }
}
