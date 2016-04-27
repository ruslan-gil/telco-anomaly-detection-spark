package com.mapr.cell.failpolicy;

public class AlwaysWorking implements FailPolicy {
    @Override
    public double failProbability(int time) {
        return 0.15;
    }
}
