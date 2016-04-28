package com.mapr.cell;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.mapr.cell.common.CDR;

import java.util.Random;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Map;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Implements a session initiation state machine
 */
public class Caller extends UntypedActor {
    enum State {IDLE, CONNECTING, LIVE}

    // how often should heartbeats be done?
    public static final int HEARTBEAT_SPREAD = 10;
    private static final double HEARTBEAT_MIN = 3;
    private static final double AVERAGE_CALL_LENGTH = 15;
    private static final double CONNECT_TIMEOUT = 4;

    private ActorRef  universe;

    private final Random rand;
    private final Queue<Messages.Log> bufferedMessages = new LinkedList<>();

    private final String id;

    private double time = 0;

    // current position
    private double x;
    private double y;

    //current destination position
    private double xDest;
    private double yDest;

    private double xSpeed;
    private double ySpeed;

    // used to broadcast to all towers
    private ActorRef towers;

    // timers to trigger actions
    private double nextCall;        // if IDLE, when should we call next
    private double refreshCall;     // if LIVE, when should we test for a new tower?
    private double endCall;         // if LIVE, when should we end the call?
    private double connectTimeout;  // if CONNECTING, when should we give up?
    private double nextHeartBeat;   // in general, when should we send the next heartbeat to all towers?

    // if we are in a call, here is our latest choice of towers
    private ActorRef currentTower;
    private String currentTowerId;

    // while connecting, this is a list of towers to try
    private Iterator<Report> live;

    // we keep a dictionary of all known towers
    private Map<String, Report> signals;
    private CDR cdr;

    private static class Report {
        // when to remove this report
        double expiration;
        Messages.SignalReport report;

        public Report(double expiration, Messages.SignalReport report) {
            this.expiration = expiration;
            this.report = report;
        }
    }

    private State currentState = State.IDLE;

    public Caller() {
        rand = new Random();
        id = String.format("%08x", rand.nextInt());
        nextHeartBeat = getNextHeartbeat(time);
        nextCall = getNextCallTime(time);
        signals = Collections.synchronizedMap(new LinkedHashMap<String, Report>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Report> eldest) {
                return (eldest.getValue().expiration < time);
            }
        });

        x = rand.nextDouble() * 20e3;
        y = rand.nextDouble() * 20e3;
        generateMoveParams();
    }


    private void generateMoveParams() {
        xDest = rand.nextDouble() * 20e3;
        yDest = rand.nextDouble() * 20e3;
        double travelTime = rand.nextDouble() * 10000;
        xSpeed = (xDest - x) / travelTime;
        ySpeed = (yDest - y) / travelTime;
    }

    private void move() {
        if (1000 > Math.sqrt(Math.pow(x - xDest, 2) + Math.pow(y - yDest, 2))) {
            generateMoveParams();
        }
        x += xSpeed;
        y += ySpeed;
        universe.tell(new Messages.Move(id, x, y));
        if (cdr == null){
            return;
        }
        cdr.setX(x);
        cdr.setY(y);
        cdr.setTime(time);
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Messages.Setup) {
            Messages.Setup setupMessage = (Messages.Setup) message;
            towers = (setupMessage).towers;
            universe = (setupMessage).universe;
        } else if (message instanceof Messages.Tick) {
            time++;
            move();
            Thread.sleep(50);
            // every so often, we need to ask for a signal report
            if (time > nextHeartBeat) {
                nextHeartBeat = getNextHeartbeat(time);
                towers.tell(new Messages.SignalReportRequest(getSelf(), x, y));
            }

            // check for timeouts of any kind
            transition(message);
        } else if (message instanceof Messages.Fail || message instanceof Messages.Connect) {
            // a tower replied, this may cause us to change our state
            transition(message);
        } else if (message instanceof Messages.SignalReport) {
            // received signal report
            Messages.SignalReport m = (Messages.SignalReport) message;
            // expiration is designed to be 2x longest heartbeat interval
            signals.put(m.towerId, new Report(time + 2 * (HEARTBEAT_MIN + HEARTBEAT_SPREAD), m));
        } else {
            unhandled(message);
        }
    }

    private double getNextHeartbeat(double time) {
        return time + HEARTBEAT_MIN + HEARTBEAT_SPREAD * rand.nextDouble();
    }

    private double getNextCallTime(double time) {
        return time + 15 + 50 * rand.nextDouble();
    }


    private void transition(Object message) {
        switch (currentState) {
            case IDLE:
                processingIdleState();
                break;
            case CONNECTING:
                processingConnectingState(message);
                break;
            case LIVE:
                processingLiveState();
                break;
        }
    }

    private void processingIdleState() {
        if (time > nextCall) {
            currentState = State.CONNECTING;
            connectTimeout = time + CONNECT_TIMEOUT;
            endCall = time - AVERAGE_CALL_LENGTH * Math.log(1 - rand.nextDouble());
            sortCandidates();
            tryNextTower();
        }
    }

    private void processingConnectingState(Object message) {

        if (time > connectTimeout && live != null && live.hasNext()) {
            System.out.printf("Timed out at %.0f,%.0f,%s\n", time, connectTimeout, message.getClass().toString());
            // no answer ... just another form of rejection
            tryNextTower();

        } else if (message instanceof Messages.Fail && live != null && live.hasNext()) {
            System.out.printf("Failed at %.0f with timeout at %.0f, %s\n", time, connectTimeout, message.getClass());
            // we were rejected
            tryNextTower();

        } else if (message instanceof Messages.Connect) {
            Messages.Connect connectMessage = (Messages.Connect) message;
            refreshCall = getNextHeartbeat(time);
            currentState = State.LIVE;
            cdr.setLastReconnectTime(time);
            currentTower = connectMessage.tower;
            currentTowerId = connectMessage.towerId;
            System.out.printf("Connected at %.0f with refresh at %.0f,%.0f to %s, %s\n", time, refreshCall, endCall, currentTowerId, message.getClass());

        } else if (message instanceof Messages.Tick) {
            // ignore
        } else {
            // failed to get anybody to talk to us
            System.out.printf("Blew out at %.0f with timeout at %.0f, %s\n", time, connectTimeout, message.getClass());
            live = null;
            currentState = State.IDLE;
        }
    }

    private void processingLiveState() {
        if (time > refreshCall) {
            System.out.printf("refreshing at %.0f\n", time);
            currentState = State.CONNECTING;

            sortCandidates();
            tryNextTower();

        } else if (time > endCall) {
            cdr.finishCDR(time);
            currentTower.tell(new Messages.Disconnect(id, cdr.cloneCDR()));
            currentTower = null;
            currentTowerId = null;
            live = null;
            currentState = State.IDLE;
            nextCall = getNextCallTime(time);
            System.out.printf("end at %.0f, next call at %.0f\n", time, nextCall);
            cdr = null;
        }
    }

    private void sortCandidates() {
        List<Report> candidates = Lists.newArrayList(signals.values());
        Collections.sort(candidates, new Ordering<Report>() {
            @Override
            public int compare(Report t0, Report t1) {
                return Double.compare(t1.report.power, t0.report.power);
            }
        });
        if (candidates.size() > 10) {
            candidates = candidates.subList(0, 10);
        }
        live = candidates.iterator();
    }

    private void tryNextTower() {
        if (!live.hasNext()){
            return;
        }
        Report r = live.next();
        connectTimeout = time + CONNECT_TIMEOUT;
        System.out.printf("At %.0f setting timeout to %.0f\n", time, connectTimeout);

        if (cdr == null){
            cdr = new CDR(id, time, x, y);
        }

        cdr.setTowerId(r.report.towerId);

        if ((currentTower != null) && (currentTower != r.report.tower)) {
            cdr.setPreviousTowerId(currentTowerId);
        } else {
            cdr.setPreviousTowerId(null);
        }

        r.report.tower.tell(new Messages.Hello(getSelf(), cdr.cloneCDR(), currentTower != null));
    }

    private void log(Messages.Log e) {
        bufferedMessages.add(e);
        while (currentTower != null && bufferedMessages.size() > 0) {
            currentTower.tell(bufferedMessages.poll());
        }
    }
}
