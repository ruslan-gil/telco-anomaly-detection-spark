package com.mapr.cell.maprdb;

import com.mapr.cell.common.Config;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newFixedThreadPool;

public class Main {

    public static void main(String[] args) {
        ExecutorService pool = newFixedThreadPool(Config.TOWER_COUNT);
        for (int i = 1; i <= Config.TOWER_COUNT; i++) {
            pool.execute(new CDRConsumer(i));
        }
    }
}
