package com.mapr.cell.failpolicy;

public class TimeBased implements FailPolicy {
    private int failTime;

    public TimeBased(int failTime) {
        this.failTime = failTime;
    }

    @Override
    public double failProbability(int time) {
        return time < failTime ? 0.15 : 1;
    }
}
