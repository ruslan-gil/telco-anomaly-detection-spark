package com.mapr.cell;

import akka.actor.UntypedActor;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mapr.cell.common.CDR;
import com.mapr.cell.common.Config;
import com.mapr.cell.failpolicy.FailPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

/**
 * Each tower is an actor that receives messages from Callers.
 */
public class Tower extends UntypedActor {
    private static final double MINIMUM_RECEIVE_POWER = -55;  // -50
    private final Random rand;

    private final int id;
    private final String sid;

    private Antenna ax;
    private FailPolicy failPolicy;

    final String TOPIC_NAME = "/telco:tower%s";
    final String INIT_NAME = "/telco:init";

    private KafkaProducer<String, String> producer;

    public Tower(int id, double x, double y, FailPolicy failPolicy) {
        producer = new KafkaProducer<>(Config.getConfig().getPrefixedProps("kafka."));
        rand = new Random();
        ax = Antenna.omni(x, y);
//        ax = Antenna.shotgun(x, y, 3.14/2, -100);
        ax.setPower(24, 1);
        this.id = id;
        this.sid = String.format("%d", id);
        this.failPolicy = failPolicy;
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Messages.Setup) {
            System.out.printf("Setup complete for tower %s\n", sid);
            producer.send(new ProducerRecord<>(INIT_NAME, ax.toJSONObject().
                    put("towerId", sid).
                    put("P_MIN", MINIMUM_RECEIVE_POWER).toString()));
        } else if (message instanceof Messages.SignalReportRequest) {
            Messages.SignalReportRequest m = (Messages.SignalReportRequest) message;
            double r = ax.distance(m.x, m.y);
            double p = ax.power(m.x, m.y);
            if (p > MINIMUM_RECEIVE_POWER) {
                m.source.tell(new Messages.SignalReport(r, p, sid, getSelf()));
            }
        } else if (message instanceof Messages.Hello) {
            Messages.Hello helloMessage = (Messages.Hello) message;
            double u = rand.nextDouble();
            if (u < failPolicy.failProbability((int) helloMessage.cdr.getTime())) {
                System.out.printf("Failed call caller %s to tower %s\n", helloMessage.cdr.getCallerId(), sid);
                helloMessage.cdr.setState(CDR.State.FAIL);
                sendToStream(helloMessage.cdr.toJSONObject());
                helloMessage.caller.tell(new Messages.Fail(sid));
            } else if (u < 0.95) {
                System.out.printf("Start call caller %s to tower %s\n", helloMessage.cdr.getCallerId(), sid);
                helloMessage.caller.tell(new Messages.Connect(sid, getSelf()));
                System.out.println("Connect CDR sent: " + helloMessage.cdr.toJSONObject());
                sendToStream(helloMessage.cdr.toJSONObject());
                if (helloMessage.reconnect) {
                    helloMessage.cdr.setState(CDR.State.RECONNECT);
                    System.out.println("Reconnect CDR sent: " + helloMessage.cdr.toJSONObject());
                    sendToStream(helloMessage.cdr.toJSONObject());
                }
            } else {
                // ignore request occasionally ... it will make the caller stronger
            }
        } else if (message instanceof Messages.Disconnect) {
            Messages.Disconnect disconnectMessage = (Messages.Disconnect) message;
            System.out.printf("Finished call caller %s to tower %s\n", disconnectMessage.callerId, sid);
            System.out.println("Finished CDR sent: " + disconnectMessage.cdr.toJSONObject());
            sendToStream(disconnectMessage.cdr.toJSONObject());
        } else {
            unhandled(message);
        }
    }


    private void sendToStream(ObjectNode objectNode) {
        producer.send(new ProducerRecord<>(
                String.format(TOPIC_NAME, sid),
                objectNode.toString()), (recordMetadata, e) -> {
                    if (e != null) {
                        System.err.println("Exception occurred while sending :(");
                        System.err.println(e.toString());
                        return;
                    }
                });
        producer.flush();
    }
}
