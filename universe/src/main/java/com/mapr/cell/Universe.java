package com.mapr.cell;

import akka.actor.*;
import akka.routing.BroadcastRouter;
import com.mapr.cell.common.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A world of actors, some of whom are callers, some of whom are towers.
 */
public class Universe extends UntypedActor {
    public static final int TOWER_COUNT = Config.TOWER_COUNT;
    public static final int USER_COUNT = 100;
    public static final int UNIVERSE_LIVE_TIME = 60;

    private static final String MOVE_TOPIC = "/telco:move";

    AtomicInteger finished = new AtomicInteger(0);
    private final ActorRef users;
    private final ActorRef towers;
    private final int total;
    private KafkaProducer<String, String> producer;
    ObjectMapper mapper = new ObjectMapper();

    public Universe(int userCount, int towerCount) {
        producer = new KafkaProducer<>(Config.getConfig().getPrefixedProps("kafka."));
        this.total = userCount + towerCount;
        users = this.getContext().actorOf(new Props((UntypedActorFactory) Caller::new)
                .withRouter(new BroadcastRouter(userCount)));
        towers = this.getContext().actorOf(new Props((UntypedActorFactory) Tower::new)
                .withRouter(new BroadcastRouter(towerCount)));

    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Messages.Start) {
            users.tell(new Messages.Setup(getSelf(), towers, users));
            towers.tell(new Messages.Setup(getSelf(), towers, users));
        } else if (message instanceof Messages.Tick) {
            users.tell(message);
        } else if (message instanceof Messages.Move) {
            System.out.println("Produce message: " + mapper.writeValueAsString(message));
            producer.send(new ProducerRecord<>(MOVE_TOPIC, mapper.writeValueAsString(message)));
        } else {
            unhandled(message);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ActorSystem system = ActorSystem.create("telco");
        ActorRef universe = system.actorOf(new Props((UntypedActorFactory) () -> new Universe(USER_COUNT, TOWER_COUNT)));

        universe.tell(new Messages.Start());

        for (int i = 0; i < UNIVERSE_LIVE_TIME; i++) {
            universe.tell(new Messages.Tick());
            Thread.sleep(100);
        }
    }
}
