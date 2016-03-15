package com.mapr.cell;

import akka.actor.*;
import akka.routing.BroadcastRouter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A world of actors, some of whom are callers, some of whom are towers.
 */
public class Universe extends UntypedActor {
    public static final int TOWER_COUNT = 1000;
    public static final int USER_COUNT = 100;

    AtomicInteger finished = new AtomicInteger(0);
    private final ActorRef users;
    private final ActorRef towers;
    private final int total;

    public Universe(int userCount, int towerCount) {
        this.total = userCount + towerCount;
        users = this.getContext().actorOf(new Props(new UntypedActorFactory() {
            @Override
            public Actor create() {
                return new Caller();
            }
        }).withRouter(new BroadcastRouter(userCount)));
        towers = this.getContext().actorOf(new Props(new UntypedActorFactory() {
            @Override
            public Actor create() {
                return new Tower();
            }
        }).withRouter(new BroadcastRouter(towerCount)));

    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Messages.Start) {
            users.tell(new Messages.Setup(getSelf(), towers, users));
            towers.tell(new Messages.Setup(getSelf(), towers, users));
        } else if (message instanceof Messages.Tick) {
            users.tell(message);
        } else {
            unhandled(message);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ActorSystem system = ActorSystem.create("telco");
        ActorRef universe = system.actorOf(new Props(new UntypedActorFactory() {
            @Override
            public Actor create() {
                return new Universe(USER_COUNT, TOWER_COUNT);
            }
        }));

        universe.tell(new Messages.Start());

        for (int i = 0; i < 10000; i++) {
            universe.tell(new Messages.Tick());
            Thread.sleep(10);
        }
    }
}
