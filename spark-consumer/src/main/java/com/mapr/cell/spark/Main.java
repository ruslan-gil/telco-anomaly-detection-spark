package com.mapr.cell.spark;

import com.google.common.collect.Iterables;
import com.mapr.cell.common.CDR;
import com.mapr.cell.common.Config;
import com.mapr.cell.common.DAO;
import com.mapr.db.MapRDB;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONObject;
import org.ojai.Document;
import scala.Tuple2;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
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


        JavaReceiverInputDStream<String> cdrsDbRecords = spark.jssc.receiverStream(new MaprDBReceiver.CDRReceiver());
        JavaDStream<CDR> dbCdrs = cdrsDbRecords.map(new Function<String, CDR>() {
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
        JavaPairDStream<String, Double> towerAllInfo = groupedByTower
                .mapValues(new Function<Iterable<CDR>, Double>() {
                    @Override
                    public Double call(Iterable<CDR> cdrs) throws Exception {
                        return (double) Iterables.size(cdrs);
                    }
                });
        JavaPairDStream<String, Double> towerTime = groupedByTower
                .mapValues(new Function<Iterable<CDR>, Double>() {
                    @Override
                    public Double call(Iterable<CDR> cdrs) throws Exception {
                        double max = Double.MIN_VALUE;
                        for(CDR cdr : cdrs) {
                            max = max<cdr.getTime() ? cdr.getTime() : max;
                        }
                        return max;
                    };
                });


        JavaPairDStream<String, Double> towerFailsGlobal = towerInfo.
                filter(new Function<Tuple2<String, CDR>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, CDR> tuple) throws Exception {
                        return tuple._2().getState() == CDR.State.FAIL;
                    }
                })
                .groupByKey()
                .mapValues(new Function<Iterable<CDR>, Double>() {
                    @Override
                    public Double call(Iterable<CDR> cdrs) throws Exception {
                        return (double) Iterables.size(cdrs);
                    }
                });

        JavaPairDStream<Tuple2<String, String>, Iterable<CDR>> groupedBySession = dbCdrs.mapToPair(new PairFunction<CDR, Tuple2<String, String>, CDR>() {
            @Override
            public Tuple2<Tuple2<String, String>, CDR> call(CDR cdr) throws Exception {
                return new Tuple2<>(new Tuple2<>(cdr.getTowerId(), cdr.getSessionId()), cdr);
            }
        }).groupByKey();

        JavaPairDStream<String, Double> sessionCountByTower = groupedBySession.mapValues(new Function<Iterable<CDR>, Double>() {
            @Override
            public Double call(Iterable<CDR> cdrs) throws Exception {
                return 1.;
            }
        }).mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,Double>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<Tuple2<String, String>, Double> tuple) throws Exception {
                return new Tuple2<>(tuple._1()._1(), tuple._2());
            }
        }).groupByKey().mapValues(new Function<Iterable<Double>, Double>() {
            @Override
            public Double call(Iterable<Double> sessions) throws Exception {
                return  (double) Iterables.size(sessions);
            }
        });



        JavaPairDStream<String, Iterable<CDR>> groupedByPreviousTower = dbCdrs.mapToPair(new PairFunction<CDR, String, CDR>() {
            @Override
            public Tuple2<String, CDR> call(CDR cdr) throws Exception {
                Tuple2<String, CDR> pair;
                if (cdr.getPreviousTowerId() != null) {
                    pair = new Tuple2<>(cdr.getPreviousTowerId(), cdr);
                } else {
                    pair = new Tuple2<>(cdr.getTowerId(), cdr);
                }
                return pair;
            }
        }).groupByKey();

        JavaPairDStream<String, Double> towerDurations = groupedByPreviousTower.
                mapValues(new Function<Iterable<CDR>, Double>() {
            @Override
            public Double call(Iterable<CDR> cdrs) throws Exception {
                double duration = 0;
                for (CDR cdr : cdrs) {
                    if (cdr.getState() == CDR.State.FINISHED || cdr.getState() == CDR.State.RECONNECT) {
                        duration += cdr.getTime() - cdr.getLastReconnectTime();
                    }
                }
                return duration;
            }
        });

        JavaReceiverInputDStream<String> statsDbRecords = spark.jssc.receiverStream(new MaprDBReceiver.StatsReceiver());

        JavaDStream<Map> dbStats= statsDbRecords.map(new Function<String, Map>() {
            @Override
            public Map call(String s) throws Exception {
                System.out.println(s);
                ObjectMapper mapper = new ObjectMapper();
                JsonFactory factory = mapper.getJsonFactory(); // since 2.1 use mapper.getFactory() instead
                JsonParser jp = factory.createJsonParser(s);
                JsonNode actualObj = mapper.readTree(jp);
                return mapper.convertValue(actualObj, Map.class);
            }
        });

        JavaPairDStream<String, Map> towerStat = dbStats.mapToPair(new PairFunction<Map, String, Map>() {
            @Override
            public Tuple2<String, Map> call(Map stat) throws Exception {
                return new Tuple2<>(stat.get("towerId").toString(), stat);
            }
        });

//        JavaPairDStream<String, Document> resultStat = new JavaPairStreamJoiner(towerStat).
//                addStats(towerAllInfo, "towerAllInfo").
//                addStats(towerFailsGlobal, "towerFails").
//                addStats(towerDurations, "towerDuration").
//                build().
//        mapValues(new Function<Map, Document>() {
//            @Override
//            public Document call(Map jsonNodes) throws Exception {
//                return MapRDB.newDocument(jsonNodes);
//            }
//        });

        JavaPairDStream<String, Document> resultStat = towerStat.join(towerAllInfo).mapValues(new Function<Tuple2<Map,Double>, Map>() {
            @Override
            public Map call(Tuple2<Map, Double> values) throws Exception {
                Map tmp = new LinkedHashMap(values._1());
                tmp.put("towerAllInfo", values._2());
                return tmp;
            }
        })
                .join(towerFailsGlobal).mapValues(new Function<Tuple2<Map,Double>, Map>() {
                    @Override
                    public Map call(Tuple2<Map, Double> values) throws Exception {
                        Map tmp = new LinkedHashMap(values._1());
                        tmp.put("towerFails", values._2());
                        return tmp;
                    }
                })
        .join(towerDurations).mapValues(new Function<Tuple2<Map,Double>, Map>() {
            @Override
            public Map call(Tuple2<Map, Double> values) throws Exception {
                Map tmp = new LinkedHashMap(values._1());
                tmp.put("towerDurations", values._2());
                return tmp;
            }
        }).join(sessionCountByTower).mapValues(new Function<Tuple2<Map,Double>, Map>() {
            @Override
            public Map call(Tuple2<Map, Double> values) throws Exception {
                Map tmp = new LinkedHashMap(values._1());
                tmp.put("sessions", values._2());
                return tmp;
            }
        }).join(towerTime).mapValues(new Function<Tuple2<Map,Double>, Map>() {
            @Override
            public Map call(Tuple2<Map, Double> values) throws Exception {
                Map tmp = new LinkedHashMap(values._1());
                tmp.put("time", values._2());
                return tmp;
            }
        }).



        mapValues(new Function<Map, Document>() {
            @Override
            public Document call(Map jsonNodes) throws Exception {
                return MapRDB.newDocument(jsonNodes);
            }
        });



        resultStat.foreachRDD(new Function<JavaPairRDD<String, Document>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Document> statsRDD) throws Exception {
                statsRDD.foreach(new VoidFunction<Tuple2<String, Document>>() {
                    @Override
                    public void call(Tuple2<String, Document> stats) throws Exception {
                        DAO.getInstance().sendTowerStats(stats._2());
                    }
                });
                return null;
            }
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

    private static class JavaPairStreamJoiner {

        JavaPairDStream<String, Map> innerStream;

        public JavaPairStreamJoiner(JavaPairDStream<String, Map> stream) {
            innerStream = stream;
        }

        public JavaPairStreamJoiner addStats(JavaPairDStream<String, Double> stream, final String paramName) {
            innerStream = chain(innerStream, stream, paramName);
            return this;
        }

        private static JavaPairDStream<String, Map> chain(JavaPairDStream<String, Map> innerStream,
                                                             JavaPairDStream<String, Double> stream,
                                                             final String paramName) {
            innerStream = innerStream.join(stream).mapValues(new Function<Tuple2<Map,Double>, Map>() {
                @Override
                public Map call(Tuple2<Map, Double> values) throws Exception {
                    Map tmp = new LinkedHashMap(values._1());
                    tmp.put(paramName, values._2());
                    return tmp;
                }
            });
            return innerStream;
        }

        public JavaPairDStream<String, Map> build() {
            JavaPairDStream<String, Map> innerStream2 = innerStream;
            innerStream = null;
            return innerStream2;
        }

    }
}
