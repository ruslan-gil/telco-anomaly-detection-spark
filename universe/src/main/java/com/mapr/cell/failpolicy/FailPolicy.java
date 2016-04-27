package com.mapr.cell.failpolicy;

public interface FailPolicy {
    double failProbability(int time);
}
