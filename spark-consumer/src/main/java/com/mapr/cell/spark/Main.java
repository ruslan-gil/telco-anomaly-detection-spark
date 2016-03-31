package com.mapr.cell.spark;

import com.google.common.collect.Iterables;
import com.mapr.cell.common.CDR;
import com.mapr.cell.common.Config;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
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

import java.util.*;

public class Main {


    public static void main(String[] args) {

        Set<String> topics = getTopics();
        Spark spark = new Spark(1000);

        JavaPairInputDStream<String, String> messages = spark.getInputStream(topics);

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

        spark.start();

    }

    private static Set<String> getTopics() {
        Set<String> topics = new HashSet<>();
        for (int i = 1; i<= Config.TOWER_COUNT; i++) {
            topics.add(Config.getTopicPath("tower" + i));
        }
        System.out.println(topics);
        return topics;
    }

    private static KafkaProducer<String, String> producer = null;

    private static KafkaProducer<String, String> getKafkaProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(Config.getConfig().getPrefixedProps("kafka."));
        }
        return producer;
    }
}
