package com.mapr.cell;

import akka.actor.UntypedActor;

import java.util.Random;

/**
 * Each tower is an actor that receives messages from Callers.
 */
public class Tower extends UntypedActor {
    private static final double MINIMUM_RECEIVE_POWER = -100;
    private final Random rand;

    private Antenna ax;
    private String id;

    public Tower() {
        rand = new Random();
        ax = Antenna.omni(rand.nextDouble() * 20e3, rand.nextDouble() * 20e3);
        ax.setPower(100, 1);
        id = String.format("%08x", rand.nextInt());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Messages.Setup) {
            System.out.printf("Setup complete for tower %s\n", id);
        } else if (message instanceof Messages.SignalReportRequest) {
            Messages.SignalReportRequest m = (Messages.SignalReportRequest) message;
            double r = ax.distance(m.x, m.y);
            double p = ax.power(m.x, m.y);
            if (p > MINIMUM_RECEIVE_POWER) {
                m.source.tell(new Messages.SignalReport(r, p, id, getSelf()));
            }
        } else if (message instanceof Messages.Hello) {
            double u = rand.nextDouble();
            if (u < 0.8) {
                ((Messages.Hello) message).caller.tell(new Messages.Connect(id, getSelf()));
            } else if (u < 0.95) {
                ((Messages.Hello) message).caller.tell(new Messages.Fail(id));
            } else {
                // ignore request occasionally ... it will make the caller stronger
            }
        } else {
            unhandled(message);
        }
    }
}
