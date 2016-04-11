package com.mapr.cell.spark;

import com.google.common.collect.Iterables;
import com.mapr.cell.common.CDR;
import com.mapr.cell.common.Config;
import com.mapr.cell.common.DAO;
import com.mapr.db.MapRDB;
import com.mapr.db.mapreduce.TableInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.dstream.InputDStream;
import org.codehaus.jettison.json.JSONObject;
import org.ojai.Document;
import org.ojai.Value;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Set;

public class Main {


    public static void main(String[] args) {

        Set<String> topics = getTopics();
        Spark spark = new Spark(3000);

        JavaPairInputDStream<String, String> messages = spark.getKafkaInputStream(topics);

        JavaDStream<CDR> cdrs = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple) throws Exception {
                return tuple._2();
            }
        }).map(new Function<String, CDR>() {
            @Override
            public CDR call(String str) throws Exception {
                return CDR.stringToCDR(str);
            }
        });

        JavaPairDStream<String, CDR> towerCDRs = cdrs.mapToPair(new PairFunction<CDR, String, CDR>(){
            @Override
            public Tuple2<String, CDR> call(CDR cdr) throws Exception {
                return new Tuple2<>(cdr.getTowerId(), cdr);
            }});

        JavaPairDStream<String, Integer> towerFails = towerCDRs.
                filter(new Function<Tuple2<String, CDR>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, CDR> tuple) throws Exception {
                return tuple._2().getState() == CDR.State.FAIL;
            }
        })
        .groupByKey()
        .mapValues(new Function<Iterable<CDR>, Integer>() {
            @Override
            public Integer call(Iterable<CDR> cdrs) throws Exception {
                return Iterables.size(cdrs);
            }
        });

        JavaPairDStream<String, Integer> towerCounts = towerCDRs.groupByKey()
        .mapValues(new Function<Iterable<CDR>, Integer>() {
            @Override
            public Integer call(Iterable<CDR> cdrs) throws Exception {
                return Iterables.size(cdrs);
            }
        });

        JavaPairDStream<String, Tuple2<Integer, Integer>> towerStatus = towerFails.join(towerCounts);

        towerStatus.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                        return new JSONObject().put("towerId", tuple._1()).put("fails", tuple._2()._1())
                                .put("total", tuple._2()._2()).toString();
                    }
                })
                .foreachRDD(new Function<JavaRDD<String>, Void>() {
                    @Override
                    public Void call(JavaRDD<String> stringJavaRDD) throws Exception {
                        stringJavaRDD.foreach(new VoidFunction<String>() {
                            @Override
                            public void call(String s) throws Exception {
                                getKafkaProducer().send(new ProducerRecord<String, String>(Config.getTopicPath(Config.FAIL_TOWER_STREAM), s));

                            }
                        });
                        return null;
                    }
                });
        messages.print();

        JavaReceiverInputDStream<String> dbRecords = spark.jssc.receiverStream(new MaprDBReceiver());
        JavaDStream<CDR> dbCdrs = dbRecords.map(new Function<String, CDR>() {
            @Override
            public CDR call(String s) throws Exception {
                return CDR.stringToCDR(s);
            }
        });

        JavaPairDStream<String, CDR> towerInfo = dbCdrs.mapToPair(new PairFunction<CDR, String, CDR>() {
            @Override
            public Tuple2<String, CDR> call(CDR cdr) throws Exception {
                return new Tuple2<>(cdr.getTowerId(), cdr);
            }
        });


        JavaPairDStream<String, Iterable<CDR>> groupedByTower = towerInfo.groupByKey();
        JavaPairDStream<String, Integer> towerAllInfo = groupedByTower
                .mapValues(new Function<Iterable<CDR>, Integer>() {
                    @Override
                    public Integer call(Iterable<CDR> cdrs) throws Exception {
                        return Iterables.size(cdrs);
                    }
                });
        JavaPairDStream<String, Double> towerDurations = groupedByTower.mapValues(new Function<Iterable<CDR>, Double>() {
            @Override
            public Double call(Iterable<CDR> cdrs) throws Exception {
                double duration = 0;
                for (CDR cdr : cdrs) {
                    if (cdr.getState() == CDR.State.FINISHED) {
                        duration += cdr.getDuration();
                    }
                }
                return duration;
            }
        });

        JavaPairDStream<String, Document> documentsPartial1 = towerAllInfo.join(towerFails).mapValues(new Function<Tuple2<Integer, Integer>, Document>() {
            @Override
            public Document call(Tuple2<Integer, Integer> values) throws Exception {
                return MapRDB.newDocument().
                        set("towerAllInfo", values._1()).
                        set("towerFails", values._2());
            }
        });

        JavaPairDStream<String, Document> documentsPartial2 = documentsPartial1.join(towerDurations).mapValues(new Function<Tuple2<Document, Double>, Document>() {
            @Override
            public Document call(Tuple2<Document, Double> values) throws Exception {
                return values._1().set("towerDurations", values._2());
            }
        });

        documentsPartial2.foreachRDD(new Function<JavaPairRDD<String, Document>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Document> statsRDD) throws Exception {
                statsRDD.foreach(new VoidFunction<Tuple2<String, Document>>() {
                    @Override
                    public void call(Tuple2<String, Document> stats) throws Exception {
                        new DAO().sendTowerStats(stats._1(), stats._2());
                    }
                });
                return null;
            }
        });


//        towerAllInfo.join(towerFailsInfo).join();


//        JavaDStream<Long> failsAmount = dbCdrs.filter(new Function<CDR, Boolean>() {
//            @Override
//            public Boolean call(CDR cdr) throws Exception {
//                return cdr.getState() == CDR.State.FAIL;
//            }
//        }).count();

        dbRecords.print();

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
