package com.mapr.cell.spark;

import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import com.mapr.cell.common.CDR;
import com.mapr.cell.common.Config;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Main {


    public static void main(String[] args) {

        Properties properties;

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
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Config.getConfig().getPrefixedProps("kafka."));

        Set<String> topics = new HashSet<>();
        for (String topic : consumer.listTopics().keySet()) {
            if (topic.startsWith(Config.getTopicPath("tower"))) {
                topics.add(topic);
            }
        }
        System.out.println(topics);

        JavaPairInputDStream<String, String> messages =
                KafkaUtils.createDirectStream(jssc, String.class, String.class,
                        Config.getConfig().getPrefixedMap("kafka."),
                        topics);

        JavaDStream<CDR> cdrs = messages.map((Function<Tuple2<String, String>, String>) Tuple2::_2)
                .map((Function<String, CDR>) CDR::stringToCDR);

        JavaPairDStream<String, CDR> towerCDRs = cdrs.mapToPair(
                (PairFunction<CDR, String, CDR>) cdr -> new Tuple2<>(cdr.getTowerId(), cdr));

        JavaPairDStream<String, Integer> towerFails = towerCDRs.
                filter((Function<Tuple2<String, CDR>, Boolean>) tuple -> tuple._2().getState() == CDR.State.FAIL)
                .groupByKey()
                .mapValues((Function<Iterable<CDR>, Integer>) Iterables::size);

        JavaPairDStream<String, Integer> towerCounts = towerCDRs.groupByKey()
                .mapValues((Function<Iterable<CDR>, Integer>) Iterables::size);

        JavaPairDStream<String, Tuple2<Integer, Integer>> towerStatus = towerFails.join(towerCounts);

        towerStatus.map((Function<Tuple2<String, Tuple2<Integer, Integer>>, String>) tuple2 ->
                new JSONObject().put("towerId", tuple2._1()).put("fails", tuple2._2()._1())
                .put("total", tuple2._2()._2()).toString())
                .foreach((Function<JavaRDD<String>, Void>) stringJavaRDD -> {
                    stringJavaRDD.foreach((VoidFunction<String>) s ->
                            getKafkaProducer().send(new ProducerRecord<>(Config.getTopicPath(Config.FAIL_TOWER_STREAM), s)));
                    return null;
                });

        messages.print();

        jssc.start();
        jssc.awaitTermination();

    }

    private static KafkaProducer<String, String> producer = null;

    private static KafkaProducer<String, String> getKafkaProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(Config.getConfig().getPrefixedProps("kafka."));
        }
        return producer;
    }
}
