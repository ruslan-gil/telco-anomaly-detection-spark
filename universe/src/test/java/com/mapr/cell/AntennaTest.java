package com.mapr.cell;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Random;

import static org.junit.Assert.*;

public class AntennaTest {
    /**
     * Creates antennas at random location and verifies invariants
     *
     * @throws Exception
     */
    @Test
    public void testOmni() throws Exception {
        Random cx = new Random();
        for (int i = 0; i < 20; i++) {
            double x = cx.nextDouble() * 20;
            double y = cx.nextDouble() * 20;
            Antenna ax = Antenna.omni(x, y);
            ax.setPower(100, 0.01);
            checkInvariants(ax, x, y);
        }
    }

    /**
     * For an omni-directional antenna at a particular location, test various invariants.
     *
     * @param ax The antenna
     * @param x0 The x-coordinate of the antenna
     * @param y0 The y-coordinate of the antenna
     */
    private void checkInvariants(Antenna ax, double x0, double y0) {
        Random rx = new Random();

        // compute expected power at distance 1 meter from antenna
        double pRef = ax.power(x0 + 1, y0);

        for (int i = 0; i < 100; i++) {
            // pick a distance from the antenna
            double r = -1000 * Math.log(1 - rx.nextDouble());
            if (r < 0.1 || r > 20000) {
                continue;
            }

            for (int j = 0; j < 100; j++) {
                // now pick lots of directions
                double theta = rx.nextDouble() * 2 * Math.PI;
                // and check the power, scaled by distance squared
                double dbm = ax.power(x0 + r * Math.cos(theta), y0 + r * Math.sin(theta));
                assertTrue(String.format("Range check for dbm: %.1f", dbm), dbm > -150);
                assertTrue(String.format("Range check for dbm: %.1f", dbm), dbm < 60);
                double px = dbm + 2 * 20 * Math.log10(r);
                assertEquals(1, pRef / px, 1e-3);
            }
        }
    }

    @Test
    public void testForwardGainRatio() throws Exception {
        double theta = Math.toRadians(30);
        Antenna ax = Antenna.shotgun(0, 0, theta, 12);
        double pBack = ax.power(Math.cos(theta + Math.PI), Math.sin(theta + Math.PI));
        double pForward = ax.power(Math.cos(theta), Math.sin(theta));
        double gain = pForward - pBack;
        assertEquals(12, gain, 1e-6);
    }

    @Test
    public void testPlotShadows() throws FileNotFoundException {
        Antenna ax = Antenna.shotgun(0, 0, Math.toRadians(30), 10);
        ax.setPower(0.1, 0.1);
        ax.setLobes(0);
        try (PrintWriter pw = new PrintWriter(new FileOutputStream("beam-1.csv"))) {
            pw.printf("x,y,px,py\n");
            for (double theta = 0; theta < 2 * Math.PI; theta += 0.01) {
                double x = Math.cos(theta);
                double y = Math.sin(theta);
                double power = Math.pow(10, ax.power(x, y) / 20);
                pw.printf("%.3f,%.3f,%.3f,%.3f\n", x, y, power * x, power * y);
            }
        }

        double x0 = -0.3;
        double y0 = 0.5;
        ax = Antenna.shotgun(x0, y0, Math.toRadians(260), 30);
        ax.setLobes(5);
        ax.setPower(0.1, 0.1);
        try (PrintWriter pw = new PrintWriter(new FileOutputStream("beam-2.csv"))) {
            pw.printf("x,y,px,py\n");
            for (double theta = 0; theta < 2 * Math.PI; theta += 0.01) {
                double x = Math.cos(theta) + x0;
                double y = Math.sin(theta) + y0;
                double power = Math.pow(10, ax.power(x, y) / 20);
                pw.printf("%.3f,%.3f,%.3f,%.3f\n", x, y, x0 + power * (x - x0), y0 + power * (y - y0));
            }
        }

        x0 = 0.5;
        y0 = -0.5;
        ax = Antenna.shotgun(x0, y0, Math.toRadians(120), 20);
        ax.setLobes(0);
        ax.setPower(0.1, 0.1);
        try (PrintWriter pw = new PrintWriter(new FileOutputStream("beam-3.csv"))) {
            pw.printf("x,y,px,py\n");
            for (double theta = 0; theta < 2 * Math.PI; theta += 0.01) {
                double x = Math.cos(theta) + x0;
                double y = Math.sin(theta) + y0;
                double power = Math.pow(10, ax.power(x, y) / 20);
                pw.printf("%.3f,%.3f,%.3f,%.3f\n", x, y, x0 + power * (x - x0), y0 + power * (y - y0));
            }
        }
    }
}