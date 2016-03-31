package com.mapr.cell.spark;


import com.mapr.cell.common.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;

import java.util.Map;
import java.util.Set;

public class Spark {
    JavaStreamingContext jssc;

    public Spark(int duration) {
        SparkConf sparkConf = new SparkConf();
        for(Map.Entry<String, String> entry : Config.getConfig().getPrefixedMap("spark.").entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }

         jssc = new JavaStreamingContext(sparkConf, new Duration(duration));
    }
    

    public JavaPairInputDStream<String, String> getInputStream(Set<String> topics){



        return KafkaUtils.createDirectStream(jssc, String.class, String.class,
                        Config.getConfig().getPrefixedMap("kafka."),
                        topics);
    }

    public void start(){
        jssc.start();
        jssc.awaitTermination();
    }

}
