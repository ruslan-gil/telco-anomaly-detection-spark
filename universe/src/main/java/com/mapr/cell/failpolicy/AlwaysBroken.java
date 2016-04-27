package com.mapr.cell.failpolicy;

public class AlwaysBroken implements FailPolicy{
    @Override
    public double failProbability(int time) {
        return 1.0;
    }
}
