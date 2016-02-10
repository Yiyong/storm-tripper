package model;

import utils.Constants;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yiguo on 1/20/16.
 */
public class SimpleReportingMessage {
    private String aggregateKey;
    private String dateInHour;

    private int impressions;
    private int clicks;
    private int errors;
    private int conversions;

    private Map<String,Map<String, Integer>> hourlyReportingMap;

    public SimpleReportingMessage(String aggregateKey, String dateInHour, int impressions, int clicks, int errors, int conversions) {
        this.aggregateKey = aggregateKey;
        this.dateInHour = dateInHour;
        this.impressions = impressions;
        this.clicks = clicks;
        this.errors = errors;
        this.conversions = conversions;

        initialHourlyReportingMap();
    }

    public SimpleReportingMessage(String aggregateKey, String dateInHour, ReportingEventType type, int count) {
        this.aggregateKey = aggregateKey;
        this.dateInHour = dateInHour;
        switch (type) {
            case IMPRESSION: {
                this.impressions = count;
                this.clicks = 0;
                this.errors = 0;
                this.conversions = 0;
                break;
            }
            case CLICK: {
                this.impressions = 0;
                this.clicks = count;
                this.errors = 0;
                this.conversions = 0;
                break;
            }
            case ERROR: {
                this.impressions = 0;
                this.clicks = 0;
                this.errors = count;
                this.conversions = 0;
                break;
            }
            case CONVERSION: {
                this.impressions = 0;
                this.clicks = 0;
                this.errors = 0;
                this.conversions = count;
                break;
            }
            default: {
                this.impressions = 0;
                this.clicks = 0;
                this.errors = 0;
                this.conversions = 0;
                break;
            }
        }

        initialHourlyReportingMap();
    }

    public String getAggregateKey() {
        return aggregateKey;
    }

    public void setAggregateKey(String aggregateKey) {
        this.aggregateKey = aggregateKey;
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

    public String getDateInHour() {
        return dateInHour;
    }

    public void setDateInHour(String dateInHour) {
        this.dateInHour = dateInHour;
    }


    private void initialHourlyReportingMap(){
        hourlyReportingMap = new HashMap<String, Map<String, Integer>>();

    }

    public Map<String, Map<String, Integer>> getHourlyReportingMap() {
        return hourlyReportingMap;
    }

    private void setHourlyReportingMap(Map<String, Map<String, Integer>> hourlyReportingMap) {
        this.hourlyReportingMap = hourlyReportingMap;
        Map<String, Integer> oneHourReportingMap = new HashMap<String, Integer>();

        oneHourReportingMap.put(Constants.IMPRESSIONS, this.impressions);
        oneHourReportingMap.put(Constants.CLICKS, this.clicks);
        oneHourReportingMap.put(Constants.ERRORS, this.errors);
        oneHourReportingMap.put(Constants.CONVERSIONS, this.conversions);

        hourlyReportingMap.put(dateInHour, oneHourReportingMap);
    }
}
