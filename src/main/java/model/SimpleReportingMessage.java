package model;

/**
 * Created by yiguo on 1/20/16.
 */
public class SimpleReportingMessage {
    private String aggregateKey;
    private long timestamp;

    private int impressions;
    private int clicks;
    private int errors;
    private int conversions;

    public SimpleReportingMessage(String aggregateKey, int impressions, int clicks, int errors, int conversions) {
        this.aggregateKey = aggregateKey;
        this.impressions = impressions;
        this.clicks = clicks;
        this.errors = errors;
        this.conversions = conversions;
    }

    public SimpleReportingMessage(String aggregateKey, ReportingEventType type, int count){
        this.aggregateKey = aggregateKey;
        switch (type) {
            case IMPRESSION:{
                this.impressions = count;
                this.clicks = 0;
                this.errors = 0;
                this.conversions = 0;
                break;
            }
            case CLICK:{
                this.impressions = 0;
                this.clicks = count;
                this.errors = 0;
                this.conversions = 0;
                break;
            }
            case ERROR:{
                this.impressions = 0;
                this.clicks = 0;
                this.errors = count;
                this.conversions = 0;
                break;
            }
            case CONVERSION:{
                this.impressions = 0;
                this.clicks = 0;
                this.errors = 0;
                this.conversions = count;
                break;
            }
            default:{
                this.impressions = 0;
                this.clicks = 0;
                this.errors = 0;
                this.conversions = 0;
                break;
            }
        }
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

    public int getErrors() {
        return errors;
    }

    public void setErrors(int errors) {
        this.errors = errors;
    }

    public int getConversions() {
        return conversions;
    }

    public void setConversions(int conversions) {
        this.conversions = conversions;
    }

}
