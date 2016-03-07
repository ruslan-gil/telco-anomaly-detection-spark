package com.mapr.cell;

import akka.actor.ActorRef;

/**
 * Classes for each kind of message
 */
public class Messages {
    /**
     * Gives users and towers access to the rest of the universe
     */
    public static class Setup {
        final ActorRef universe;
        final ActorRef towers;
        final ActorRef users;

        public Setup(ActorRef universe, ActorRef towers, ActorRef users) {
            this.universe = universe;
            this.towers = towers;
            this.users = users;
        }
    }

    /**
     * Indicates that the universe should start the simulation
     */
    public static class Start {}

    /**
     * Indicates the passage of a bit of simulation time. Sent to all users.
     */
    public static class Tick {}

    /**
     * Requests a signal report. Sent from caller to all towers.
     */
    public static class SignalReportRequest {
        final ActorRef source;
        final double x;
        final double y;

        public SignalReportRequest(ActorRef source, double x, double y) {
            this.source = source;
            this.x = x;
            this.y = y;
        }
    }

    /**
     * Signal report from tower to caller.
     */
    public static class SignalReport {
        final double distance;
        final double power;
        final String towerId;
        final ActorRef tower;

        public SignalReport(double distance, double power, String towerId, ActorRef tower) {
            this.distance = distance;
            this.power = power;
            this.towerId = towerId;
            this.tower = tower;
        }
    }

    /**
     * Tells the caller that the tower will not accept the connection. Sent in response to Hello.
     */
    public static class Fail {
        final String towerId;

        public Fail(String towerId) {
            this.towerId = towerId;
        }
    }

    /**
     * Tells the caller that the tower has accepted the connection. Sent in response to Hello.
     */
    public static class Connect {
        final String towerId;
        final ActorRef tower;

        public Connect(String towerId, ActorRef tower) {
            this.towerId = towerId;
            this.tower = tower;
        }
    }

    /**
     * Sent by the caller to indicate it is disconnecting the current call.
     */
    public static class Disconnect {
        String callerId;

        public Disconnect(String callerId) {
            this.callerId = callerId;
        }
    }

    /**
     * Sent by the caller to ask the tower if it will accept a call.
     */
    public static class Hello {
        public final ActorRef caller;

        public Hello(ActorRef self) {
            this.caller = self;
        }
    }

    /**
     * Sent by the caller to record data (essentially a call data record)
     */
    public static class Log {
    }
}
