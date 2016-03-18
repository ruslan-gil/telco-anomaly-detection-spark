package com.mapr.cell.spark;

import com.google.common.io.Resources;
import com.mapr.cell.CDR;
import com.mapr.cell.Utils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.collections.Predicate;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import org.json.JSONException;
import scala.Tuple2;

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

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Config.getConfig().getPrefixedProps("kafka."));
        Set<String> topics = consumer.listTopics().keySet();
        System.out.println(topics);

        JavaPairInputDStream<String, String> messages =
                KafkaUtils.createDirectStream(jssc, String.class, String.class,
                        Config.getConfig().getPrefixedMap("kafka."),
                        topics);
        JavaDStream<CDR> cdrs = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        }).map(new Function<String, CDR>() {
            @Override
            public CDR call(String jsonCDR) {
                return CDR.stringToCDR(jsonCDR);
            }
        });


        JavaPairDStream<String, List<CDR>> towerCDRs = cdrs.mapToPair(
                new PairFunction<CDR, String, List<CDR>>() {
                    @Override
                    public Tuple2<String, List<CDR>> call(CDR cdr) throws Exception {
                        return new Tuple2<>(cdr.getTowerId(), Collections.singletonList(cdr));
                    }
                }).reduceByKey(new Function2<List<CDR>, List<CDR>, List<CDR>>() {
                                    @Override
                                    public List<CDR> call(List<CDR> l1, List<CDR> l2) {
                                        return ListUtils.union(l1, l2);
                                    }
                                });
//        TODO: check fails count and refactor
        JavaPairDStream<String, Double> towerFails = towerCDRs.mapValues(new Function<List<CDR>, Double>() {
            @Override
            public Double call(List<CDR> cdrs) throws JSONException {
                List<CDR> filtredList = Utils.filter(cdrs, new Predicate() {
                    @Override
                    public boolean evaluate(Object o) {
                        return ((CDR) o).getState() != CDR.State.FAIL;
                    }
                });
                return (double) (filtredList.size() / cdrs.size());
            }
        });

        towerCDRs.print();
        towerFails.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
