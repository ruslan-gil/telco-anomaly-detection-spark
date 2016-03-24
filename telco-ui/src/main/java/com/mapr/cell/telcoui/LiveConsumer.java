package com.mapr.cell.telcoui;

import com.mapr.cell.common.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class LiveConsumer {

    public LiveConsumer() {
        List<BaseConsumer> consumers = new ArrayList<>();
        consumers.add(new InitConsumer(Config.INIT_TOPIC_NAME));
        consumers.add(new MoveConsumer(Config.MOVE_TOPIC_NAME));
        consumers.forEach(BaseConsumer::start);
    }

    public abstract class BaseConsumer extends Thread {
        private KafkaConsumer<String, String> consumer;

        public BaseConsumer(String topic) {
            consumer = new KafkaConsumer<>(Config.getConfig().getPrefixedProps("kafka."));
            consumer.subscribe(Arrays.asList(topic));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                consumer.close();
            }));
        }

        @Override
        public void run() {
            long pollTimeOut = 10;

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
                if (!records.isEmpty()) {
                    processRecords(records);
                    consumer.commitAsync();
                }
            }
        }

        protected abstract void processRecords(ConsumerRecords<String, String> records);
    }


    public class InitConsumer extends BaseConsumer {

        public InitConsumer(String topic) {
            super(topic);
        }

        protected void processRecords(ConsumerRecords<String, String> records) {
            for(ConsumerRecord<String, String> record : records) {
                try {
                    JSONObject recordJSON = new JSONObject(record.value());
                    onNewInitData(recordJSON);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public class MoveConsumer extends BaseConsumer {

        public MoveConsumer(String topic) {
            super(topic);
        }

        protected void processRecords(ConsumerRecords<String, String> records) {
            for(ConsumerRecord<String, String> record : records) {
                try {
                    JSONObject recordJSON = new JSONObject(record.value());
                    onNewMoveData(recordJSON);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void onNewInitData(JSONObject arg) {
        listeners.forEach((l) -> l.onNewInitData(arg));
    }


    public void onNewMoveData(JSONObject arg) {
        listeners.forEach((l) -> l.onNewMoveData(arg));
    }

    private Set<Listener> listeners = Collections.synchronizedSet(new HashSet<>());

    public void subscribe(Listener l) {
        listeners.add(l);
    }

    public void unsubscribe(Listener l) {
        listeners.remove(l);
    }

    public interface Listener {
        void onNewInitData(JSONObject data);
        void onNewMoveData(JSONObject data);
    }

}