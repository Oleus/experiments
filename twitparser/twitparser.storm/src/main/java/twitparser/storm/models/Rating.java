package twitparser.storm.models;

import java.io.Serializable;

public class Rating implements Serializable {
    private long positive;
    private long negative;

    public long getPositive() {
        return positive;
    }

    public void incrementPositive() {
        this.positive++;
    }

    public long getNegative() {
        return negative;
    }

    public void incrementNegative() {
        this.negative++;
    }

    @Override
    public String toString() {
        return "pos " + positive + ", neg " + negative;
    }
}
