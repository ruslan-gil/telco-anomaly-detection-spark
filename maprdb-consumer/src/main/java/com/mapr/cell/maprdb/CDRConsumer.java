package com.mapr.cell.maprdb;

import com.mapr.cell.common.Config;
import com.mapr.cell.common.DAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

class CDRConsumer extends Thread {
    private KafkaConsumer<String, String> consumer;
    private String GROUP_ID = "cdr-consumer";
    private DAO dao;

    CDRConsumer(int id) {
        Properties props = Config.getConfig().getPrefixedProps("kafka.");
        props.setProperty(Config.KAFKA_GROUP_ID, GROUP_ID);
        dao = new DAO();
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(Config.getTowerStream(id)));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
        }));
    }

    @Override
    public void start() {
        long pollTimeOut = 10;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
            if (!records.isEmpty()) {
                for(ConsumerRecord<String, String> cdr : records) {
                    dao.addCDR(cdr.value());
                }
                consumer.commitAsync();
            }
        }
    }
}