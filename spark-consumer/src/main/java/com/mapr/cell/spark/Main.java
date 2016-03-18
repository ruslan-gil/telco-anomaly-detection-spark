package com.mapr.cell.spark;

import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Main {


    public static void main(String[] args) {

        Properties properties = null;

        try (InputStream props = Resources.getResource("config.conf").openStream()) {
            properties = new Properties();
            properties.load(props);
        } catch (IOException e) {
            e.printStackTrace();
        }

        SparkConf sparkConf = new SparkConf();
        for(Map.Entry<String, String> entry : Config.getConfig().getPrefixedMap("spark.").entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }

        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

//        Map<String, Integer> topicMap = new HashMap<>();
//        String[] topics = args[2].split(",");
//        for (String topic: topics) {
//            topicMap.put(topic,  NUM_OF_THREADS_PER_STREAM);
//        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Config.getConfig().getPrefixedProps("kafka."));
        Set<String> topics = consumer.listTopics().keySet();
        System.out.println(topics);

        assert properties != null;
        JavaPairInputDStream<String, String> messages =
                KafkaUtils.createDirectStream(jssc, String.class, String.class,
                        Config.getConfig().getPrefixedMap("kafka."),
                        topics);

        messages.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
