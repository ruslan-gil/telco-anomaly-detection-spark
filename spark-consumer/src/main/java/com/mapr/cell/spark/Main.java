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
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.codehaus.jettison.json.JSONObject;
import org.ojai.Document;
import scala.Tuple2;

import java.util.*;

public class Main {
    public static final int SINGLE_RDD_DURATION = 3000;
    public static final int WINDOW_DURATION = 6000;
    public static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {

        Set<String> topics = getTopics();
        Spark spark = new Spark(SINGLE_RDD_DURATION);

        JavaPairInputDStream<String, String> messages = spark.getKafkaInputStream(topics);

        JavaDStream<CDR> cdrs = messages.
                map((Function<Tuple2<String, String>, String>) Tuple2::_2).
                map((Function<String, CDR>) CDR::stringToCDR);

        // Windowed stream
        JavaDStream<CDR> windowCDR = cdrs.
                window(new Duration(WINDOW_DURATION), new Duration(SINGLE_RDD_DURATION));

        JavaPairDStream<String, CDR> windowTowerCDRs = towerIdWithCDRs(windowCDR);

        JavaPairDStream<String, Integer> windowTowerFails = filterCDRByFail(windowTowerCDRs);

        JavaPairDStream<String, Integer> windowTowerCounts = towerCounts(windowTowerCDRs);

        JavaPairDStream<String, Tuple2<Integer, Integer>> windowTowerStatus = windowTowerFails.join(windowTowerCounts);


        // Stream
        JavaPairDStream<String, CDR> towerCDRs = towerIdWithCDRs(cdrs);

        JavaPairDStream<String, Integer> towerFails = filterCDRByFail(towerCDRs);

        JavaPairDStream<String, Integer> towerCounts = towerCounts(towerCDRs);

        JavaPairDStream<String, Tuple2<Integer, Integer>> towerStatus = towerFails.join(towerCounts);

        towerStatus.map((Function<Tuple2<String, Tuple2<Integer, Integer>>, String>) tuple ->
                new JSONObject().
                        put("towerId", tuple._1()).
                        put("fails", tuple._2()._1()).
                        put("type", "failsPercent").
                        put("total", tuple._2()._2()).toString()).
                foreachRDD((Function<JavaRDD<String>, Void>) stringJavaRDD -> {
                    stringJavaRDD.foreach((VoidFunction<String>) s ->
                            getKafkaProducer().send(new ProducerRecord<>(Config.getTopicPath(Config.FAIL_TOWER_STREAM), s)));
                    return null;
                });

        // MapRDB
        JavaReceiverInputDStream<String> cdrsDbRecords = spark.jssc.receiverStream(new MaprDBReceiver.CDRReceiver());


        JavaDStream<CDR> dbCdrs = cdrsDbRecords.map((Function<String, CDR>) CDR::stringToCDR);

        JavaPairDStream<String, CDR> towerInfo = dbCdrs.mapToPair((PairFunction<CDR, String, CDR>) cdr ->
                new Tuple2<>(cdr.getTowerId(), cdr));

        JavaPairDStream<String, Iterable<CDR>> groupedByTower = towerInfo.groupByKey();


        JavaPairDStream<String, Double> towerAllInfo = groupedByTower
                .mapValues((Function<Iterable<CDR>, Double>) cdrs1 -> (double) Iterables.size(cdrs1));

        JavaPairDStream<String, Double> towerTime = groupedByTower
                .mapValues((Function<Iterable<CDR>, Double>) cdrs1 -> {
                    double max = Double.MIN_VALUE;
                    for(CDR cdr : cdrs1) {
                        max = max<cdr.getTime() ? cdr.getTime() : max;
                    }
                    return max;
                });

        JavaPairDStream<String, Double> towerFailsGlobal = towerInfo.
                filter((Function<Tuple2<String, CDR>, Boolean>) tuple -> tuple._2().getState() == CDR.State.FAIL)
                .groupByKey()
                .mapValues((Function<Iterable<CDR>, Double>) cdrs1 -> (double) Iterables.size(cdrs1));

        JavaPairDStream<Tuple2<String, String>, Iterable<CDR>> groupedBySession = dbCdrs.mapToPair((PairFunction<CDR, Tuple2<String, String>, CDR>) cdr ->
                new Tuple2<>(new Tuple2<>(cdr.getTowerId(), cdr.getSessionId()), cdr)).groupByKey();

        JavaPairDStream<String, Double> sessionCountByTower = groupedBySession.mapValues((Function<Iterable<CDR>, Double>) cdrs1 ->
                1.).mapToPair((PairFunction<Tuple2<Tuple2<String, String>, Double>, String, Double>) tuple ->
                new Tuple2<>(tuple._1()._1(), tuple._2())).groupByKey().mapValues((Function<Iterable<Double>, Double>) sessions ->
                (double) Iterables.size(sessions));

        JavaPairDStream<String, Iterable<CDR>> groupedByPreviousTower = dbCdrs.mapToPair((PairFunction<CDR, String, CDR>) cdr -> {
            Tuple2<String, CDR> pair;
            if (cdr.getPreviousTowerId() != null) {
                pair = new Tuple2<>(cdr.getPreviousTowerId(), cdr);
            } else {
                pair = new Tuple2<>(cdr.getTowerId(), cdr);
            }
            return pair;
        }).groupByKey();

        JavaPairDStream<String, Double> towerDurations = groupedByPreviousTower.
                mapValues((Function<Iterable<CDR>, Double>) cdrs1 -> {
                    double duration = 0;
                    for (CDR cdr : cdrs1) {
                        if (cdr.getState() == CDR.State.FINISHED || cdr.getState() == CDR.State.RECONNECT) {
                            duration += cdr.getTime() - cdr.getLastReconnectTime();
                        }
                    }
                    return duration;
                });

        // MapRDB stats by tower
        JavaReceiverInputDStream<String> statsDbRecords = spark.jssc.receiverStream(new MaprDBReceiver.StatsReceiver());

        JavaDStream<Map> dbStats= statsDbRecords.map((Function<String, Map>) s -> {
            JsonFactory factory = MAPPER.getJsonFactory(); // since 2.1 use mapper.getFactory() instead
            JsonParser jp = factory.createJsonParser(s);
            JsonNode actualObj = MAPPER.readTree(jp);
            Map map = MAPPER.convertValue(actualObj, Map.class);
            System.out.println(map.toString());
            return map;
        });

        JavaPairDStream<String, Map> towerStat = dbStats.mapToPair((PairFunction<Map, String, Map>) stat ->
                new Tuple2<>(stat.get("towerId").toString(), stat));

        // Window calculations
        JavaPairDStream<String, Tuple2<Double, Double>> windowFailCalculate = dbStats
                .filter((Function<Map, Boolean>) map -> (map.get("towerFailsHistory") != null))
                .filter((Function<Map, Boolean>) map -> ((Integer) map.get("simulationId") == DAO.getInstance().getLastSimulationID()))
                .filter((Function<Map, Boolean>) map -> {
                    List<Double> failsHistory = MAPPER.readValue(map.get("towerFailsHistory").toString(), new TypeReference<List<Double>>(){});
                    return (!(failsHistory.size() < 3));
                })
                .mapToPair((PairFunction<Map, String, Tuple2<Double, Double>>) stat -> {
                    List<Double> failsHistory = MAPPER.readValue(stat.get("towerFailsHistory").toString(), new TypeReference<List<Double>>() {
                    });
                    Double m = calcAvg(failsHistory);
                    Double sigma = calcSigma(failsHistory, m);
                    return new Tuple2<>(stat.get("towerId").toString(), new Tuple2<>(m, sigma));
                });

        JavaPairDStream<String, Double> towerErrorPercent = windowTowerFails.join(windowFailCalculate)
            .mapToPair((PairFunction<Tuple2<String, Tuple2<Integer, Tuple2<Double, Double>>>,String, Double>) stats -> {
                    double avg = stats._2()._2()._1();
                    double sigma = stats._2()._2()._2();
                    double fails = stats._2()._1();
                    Double result = (Math.abs(fails - avg) / (3 * sigma));
                    return new Tuple2<>(stats._1(), result.doubleValue());
            });

        towerErrorPercent
                .filter((Function<Tuple2<String,Double>, Boolean>) val -> (val._2() >= 1))
                .map((Function<Tuple2<String, Double>, String>) tuple ->
                         new JSONObject()
                                .put("towerId", tuple._1())
                                .put("percentOfFails", tuple._2())
                                .put("type", "deviation").toString()
                ).foreachRDD((Function<JavaRDD<String>, Void>) stringJavaRDD -> {
            stringJavaRDD.foreach((VoidFunction<String>) s ->
                getKafkaProducer().send(new ProducerRecord<>(Config.getTopicPath(Config.FAIL_TOWER_STREAM), s)));
            return null;
        });


        // Join tower stat into single document
        JavaPairDStream<String, Document> resultStat = towerStat.join(towerAllInfo)
                            .mapValues((Function<Tuple2<Map, Double>, Map>) values -> {
            Map tmp = new LinkedHashMap(values._1());
            tmp.put("towerAllInfo", values._2());
            return tmp;
        })
                .join(towerFailsGlobal).mapValues((Function<Tuple2<Map, Double>, Map>) values -> {
                    Map tmp = new LinkedHashMap(values._1());
                    tmp.put("towerFails", values._2());
                    return tmp;
                })
                .join(towerDurations).mapValues((Function<Tuple2<Map, Double>, Map>) values -> {
                    Map tmp = new LinkedHashMap(values._1());
                    tmp.put("towerDurations", values._2());
                    return tmp;
                }).join(sessionCountByTower).mapValues((Function<Tuple2<Map, Double>, Map>) values -> {
                    Map tmp = new LinkedHashMap(values._1());
                    tmp.put("sessions", values._2());
                    return tmp;
                }).join(towerTime).mapValues((Function<Tuple2<Map, Double>, Map>) values -> {
                    Map tmp = new LinkedHashMap(values._1());
                    tmp.put("time", values._2());
                    return tmp;
                }).join(windowTowerStatus).mapValues((Function<Tuple2<Map, Tuple2<Integer, Integer>>, Map>) values -> {
                    List means;
                    Map tmp = new LinkedHashMap(values._1());

                    if (values._1.containsKey("towerFailsHistory")) {
                        means = (List) values._1().get("towerFailsHistory");
                    } else {
                        means = new ArrayList();
                    }
                    means.add(values._2()._1().doubleValue()/values._2()._2());
                    tmp.put("towerFailsHistory", means);
                    return tmp;
                })
        .mapValues((Function<Map, Document>) MapRDB::newDocument);

        resultStat.foreachRDD((Function<JavaPairRDD<String, Document>, Void>) statsRDD -> {
            statsRDD.foreach((VoidFunction<Tuple2<String, Document>>) stats -> DAO.getInstance().sendTowerStats(stats._2()));
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

    private static JavaPairDStream<String, Integer> filterCDRByFail( JavaPairDStream<String, CDR> towerCDRs ){
        return towerCDRs
                .filter((Function<Tuple2<String, CDR>, Boolean>) tuple -> tuple._2().getState() == CDR.State.FAIL)
                .groupByKey()
                .mapValues((Function<Iterable<CDR>, Integer>) Iterables::size);
    }

    private static JavaPairDStream<String, CDR> towerIdWithCDRs(JavaDStream<CDR> cdrs) {
        return cdrs.mapToPair((PairFunction<CDR, String, CDR>) cdr -> new Tuple2<>(cdr.getTowerId(), cdr));
    }

    private static JavaPairDStream<String, Integer> towerCounts(JavaPairDStream<String, CDR> towerCDRs) {
        return towerCDRs
                .groupByKey()
                .mapValues((Function<Iterable<CDR>, Integer>) Iterables::size);
    }

    private static Double calcAvg(List<Double> list){
        return list
                .stream()
                .mapToDouble(a -> a)
                .average()
                .getAsDouble();
    }

    private static Double calcSigma(List<Double> list, Double m){
        return Math.sqrt(list
                            .stream()
                            .mapToDouble(a -> Math.pow((a - m), 2))
                            .sum());
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
