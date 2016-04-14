package com.mapr.cell.telcoui;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.mapr.cell.common.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.*;

public class LiveConsumer {

    public LiveConsumer() {
        List<BaseConsumer> consumers = new ArrayList<>();
        consumers.add(new InitConsumer(Config.INIT_TOPIC_NAME));
        consumers.add(new MoveConsumer(Config.MOVE_TOPIC_NAME));
        consumers.add(new StatusConsumer(Config.FAIL_TOWER_STREAM));
        consumers.add(new EventConsumer(Config.EVENT_TOPIC_NAME));
        for( int i=1 ; i<=Config.TOWER_COUNT;i++) {
            consumers.add(new TowerConsumer(Config.getTowerStream(i)));
        }
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
                    JsonNode node = new ObjectMapper().readTree(record.value());
                    onNewInitData(node);
                } catch (IOException e) {
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
                    JsonNode node = new ObjectMapper().readTree(record.value());
                    onNewMoveData(node);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public class StatusConsumer extends BaseConsumer {

        public StatusConsumer(String topic) {
            super(topic);
        }

        protected void processRecords(ConsumerRecords<String, String> records) {
            for(ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode node = new ObjectMapper().readTree(record.value());
                    onNewStatusData(node);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public class TowerConsumer extends BaseConsumer {

        public TowerConsumer(String topic) {
            super(topic);
        }

        protected void processRecords(ConsumerRecords<String, String> records) {
            for(ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode node = new ObjectMapper().readTree(record.value());
                    onNewTowerData(node);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public class EventConsumer extends BaseConsumer {

        public EventConsumer(String topic) {
            super(topic);
        }

        protected void processRecords(ConsumerRecords<String, String> records) {
            for(ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode node = new ObjectMapper().readTree(record.value());
                    onNewEventData(node);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private void onNewTowerData(JsonNode recordJSON) {
        listeners.forEach((l) -> l.onNewTowerData(recordJSON));
    }

    public void onNewInitData(JsonNode arg) {
        listeners.forEach((l) -> l.onNewInitData(arg));
    }

    public void onNewEventData(JsonNode arg) {
        listeners.forEach((l) -> l.onNewEventData(arg));
    }

    public void onNewMoveData(JsonNode arg) {
        listeners.forEach((l) -> l.onNewMoveData(arg));
    }
    public void onNewStatusData(JsonNode arg) {
        listeners.forEach((l) -> l.onNewStatusData(arg));
    }

    private Set<Listener> listeners = Collections.synchronizedSet(new HashSet<>());

    public void subscribe(Listener l) {
        listeners.add(l);
    }

    public void unsubscribe(Listener l) {
        listeners.remove(l);
    }

    public interface Listener {
        void onNewInitData(JsonNode data);
        void onNewMoveData(JsonNode data);
        void onNewStatusData(JsonNode data);
        void onNewTowerData(JsonNode data);
        void onNewEventData(JsonNode data);
    }

}