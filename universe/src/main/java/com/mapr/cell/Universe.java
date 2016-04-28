package com.mapr.cell;

import akka.actor.*;
import akka.routing.BroadcastRouter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mapr.cell.common.Config;
import com.mapr.cell.common.DAO;
import com.mapr.cell.common.Events;
import com.mapr.cell.failpolicy.AlwaysBroken;
import com.mapr.cell.failpolicy.AlwaysWorking;
import com.mapr.cell.failpolicy.TimeBased;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Random;

/**
 * A world of actors, some of whom are callers, some of whom are towers.
 */
public class Universe extends UntypedActor {
    public static final int TOWER_COUNT = Config.TOWER_COUNT;
    public static final int USER_COUNT = 100;
    public static final int UNIVERSE_LIVE_TIME = 60*10;

    private final ActorRef users;
    private final ActorRef towers;
    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper = new ObjectMapper();

    public Universe(int userCount, int towerCount) {
        producer = new KafkaProducer<>(Config.getConfig().getPrefixedProps("kafka."));

        sendEventSync(Events.SIMULATION_STARTS);
        DAO.getInstance().newSimulation();

        users = this.getContext().actorOf(new Props((UntypedActorFactory) Caller::new)
                .withRouter(new BroadcastRouter(userCount)));

        UntypedActorFactory factory = new TowerActorFactory();
        towers = this.getContext().actorOf(new Props(factory).withRouter(new BroadcastRouter(towerCount)));
    }

    public void sendEventSync(Events event) {
        ObjectNode object = mapper.createObjectNode();
        object.put(event.name(), event.toString());
        producer.send(new ProducerRecord<>(Config.getTopicPath(Config.EVENT_TOPIC_NAME),
                object.toString()));
        producer.flush();
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Messages.Start) {
            users.tell(new Messages.Setup(getSelf(), towers, users));
            towers.tell(new Messages.Setup(getSelf(), towers, users));
        } else if (message instanceof Messages.Tick) {
            users.tell(message);
        } else if (message instanceof Messages.Move) {
            producer.send(new ProducerRecord<>(Config.getTopicPath(Config.MOVE_TOPIC_NAME), mapper.writeValueAsString(message)));
        } else {
            unhandled(message);
        }
    }

    public static void main(String[] args) throws InterruptedException {

        ActorSystem system = ActorSystem.create("telco");
        ActorRef universe = system.actorOf(new Props((UntypedActorFactory) () -> new Universe(USER_COUNT, TOWER_COUNT)));

        universe.tell(new Messages.Start());

        if (args[0].equals("true")) {
            while (true) {
                universe.tell(new Messages.Tick());
                Thread.sleep(500);
            }
        } else {
            for (int i = 0; i < UNIVERSE_LIVE_TIME; i++) {
                universe.tell(new Messages.Tick());
                Thread.sleep(500);
            }
        }
    }

    private static class TowerActorFactory implements UntypedActorFactory {
        public static final double SHUFFLE = 20e3 / 10;
        private static int id = 0;
        private static double x;
        private static double y;

        final static double row = 3;
        final static double gap = 20e3 / (row + 1);

        private static Random rand = new Random();

        public TowerActorFactory() {
            x = gap;
            y = gap;
        }

        @Override
        public Actor create() {
            return getActor();
        }

        private synchronized static Actor getActor() {
            id ++;
            Tower tower;

            x += rand.nextDouble() * (SHUFFLE) - ((SHUFFLE) / 2);
            y += rand.nextDouble() * (SHUFFLE) - ((SHUFFLE) / 2);

            if (id == 1) {
                tower = new Tower(id, x, y, new TimeBased(60*3));
            } else if (id == 2) {
                tower = new Tower(id, x, y, new AlwaysBroken());
            } else if (id == 6) {
                tower = new Tower(id, x, y, new TimeBased(60*5));
            } else {
                tower = new Tower(id, x, y, new AlwaysWorking());
            }

            x += gap;
            if (id % row == 0 && id > 0) {
                y += gap;
                x = gap;
            }
            return tower;
        }
    }
}
