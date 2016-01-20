package model;

/**
 * Created by yiguo on 1/20/16.
 */
public class SimpleReportingMessage {
    private String aggregateKey;
    private long timestamp;
    private int impressions;
    private int clicks;

    public SimpleReportingMessage(String aggregateKey, int impressions, int clicks) {
        this.aggregateKey = aggregateKey;
        this.impressions = impressions;
        this.clicks = clicks;
    }

    public String getAggregateKey() {
        return aggregateKey;
    }

    public void setAggregateKey(String aggregateKey) {
        this.aggregateKey = aggregateKey;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getImpressions() {
        return impressions;
    }

    public void setImpressions(int impressions) {
        this.impressions = impressions;
    }

    public int getClicks() {
        return clicks;
    }

    public void setClicks(int clicks) {
        this.clicks = clicks;
    }
}
