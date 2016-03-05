package com.mapr.cell;

/**
 * An antenna represents a single RF radiator with a tuned cardioid response and a limited
 * number of shadow creating objects.
 */
public class Antenna {
    // location of the antenna
    private double x0;
    private double y0;

    // orientation of the antenna forward gain direction
    private double theta0;

    // there will be 2*lobes - 1 side lobes and one forward gain lobe
    private double lobes;

    // forward/back ratio for antenna will be (1+e)/(1-e)
    private double eccentricity;

    // semi-major axis is set to put the largest value at theta = 0 to 1
    private double scale;

    // radiated power at distance r0
    private double p0;

    // square of reference distance. We should normally be farther than r0 from source
    private double r0Squared;

    public Antenna() {
        this(0.9, 3, 200e-3, 0.01, 0, 0, 0);
    }

    public Antenna(double eccentricity, double lobes, double p0, double r0, double theta0, double x0, double y0) {
        this.eccentricity = eccentricity;
        this.scale = 1 / (1 + eccentricity);
        this.lobes = lobes;
        this.p0 = p0;
        this.r0Squared = r0 * r0;
        this.theta0 = theta0;
        this.x0 = x0;
        this.y0 = y0;
    }

    public static Antenna omni(double x0, double y0) {
        return new Antenna(0, 0, 200e-3, 0.01, 0, x0, y0);
    }

    public static Antenna shotgun(double x0, double y0, double theta0, double gainDB) {
        double ratio = Math.pow(10.0, gainDB / 20);
        return new Antenna((ratio - 1) / (ratio + 1), 3, 200e-3, 0.01, theta0, x0, y0);
    }

    public void setPower(double p0, double r0) {
        this.p0 = p0;
        this.r0Squared = r0 * r0;
    }

    /**
     * Returns the power level received at a particular location.
     *
     * @param x X coordinate of the receiver location
     * @param y Y coordinate of the receiver location
     * @return Power in dbm at receiving location.
     */
    public double power(double x, double y) {
        double rSquared = (x - x0) * (x - x0) + (y - y0) * (y - y0);
        if (rSquared <= r0Squared) {
            // if less than reference distance just give reference power
            // to avoid problems with atan(0,0)
            return dbm(p0);
        } else {
            double theta = Math.atan2(y - y0, x - x0) - theta0;
            return dbm(antennaGain(theta) * r0Squared / rSquared * p0);
        }
    }

    /**
     * Returns the antenna gain in any particular direction
     *
     * @param theta
     * @return
     */
    public double antennaGain(double theta) {
        double directivity = scale * (1 - eccentricity * eccentricity) / (1 - eccentricity * Math.cos(theta));
        double cardiod = Math.abs(Math.cos(lobes * theta));
        return cardiod * directivity;
    }

    private double dbm(double power) {
        return 20 * Math.log10(power / 1e-3);
    }

    public void setLobes(int lobes) {
        this.lobes = lobes;
    }
}
