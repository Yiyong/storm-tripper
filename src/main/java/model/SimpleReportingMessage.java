package model;

import utils.Constants;

import java.util.*;

/**
 * Created by yiguo on 1/20/16.
 */
public class SimpleReportingMessage {
    private String aggregateKey;

    private int impressions;
    private int clicks;
    private int errors;
    private int conversions;

    private Map<String, Map<String, Integer>> hourlyReportingMap;

    public SimpleReportingMessage(String aggregateKey, int impressions, int clicks, int errors, int conversions, Map<String, Map<String, Integer>> hourlyReportingMap) {
        this.aggregateKey = aggregateKey;
        this.impressions = impressions;
        this.clicks = clicks;
        this.errors = errors;
        this.conversions = conversions;
        this.hourlyReportingMap = hourlyReportingMap;
    }

    public SimpleReportingMessage(String aggregateKey, String dateInHour, int impressions, int clicks, int errors, int conversions) {
        this.aggregateKey = aggregateKey;
        this.impressions = impressions;
        this.clicks = clicks;
        this.errors = errors;
        this.conversions = conversions;

        initialHourlyReportingMap(dateInHour);
    }

    public SimpleReportingMessage(String aggregateKey, String dateInHour, ReportingEventType type, int count) {
        this.aggregateKey = aggregateKey;
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

        initialHourlyReportingMap(dateInHour);
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

    private void initialHourlyReportingMap(String dateInHour) {
        hourlyReportingMap = new HashMap<String, Map<String, Integer>>();
        Map<String, Integer> oneHourReportingMap = new HashMap<String, Integer>();

        oneHourReportingMap.put(Constants.IMPRESSIONS, this.impressions);
        oneHourReportingMap.put(Constants.CLICKS, this.clicks);
        oneHourReportingMap.put(Constants.ERRORS, this.errors);
        oneHourReportingMap.put(Constants.CONVERSIONS, this.conversions);

        hourlyReportingMap.put(dateInHour, oneHourReportingMap);
    }

    public Map<String, Map<String, Integer>> getHourlyReportingMap() {
        return hourlyReportingMap;
    }

    private void setHourlyReportingMap(Map<String, Map<String, Integer>> hourlyReportingMap) {
        this.hourlyReportingMap = hourlyReportingMap;
    }

    public static Map<String, Map<String, Integer>> mergeHourlyReportingMap(Map<String, Map<String, Integer>> map1, Map<String, Map<String, Integer>> map2) {
        Set<String> hourSet = new HashSet<String>();
        for(String hour : map1.keySet()){
            hourSet.add(hour);
        }
        for(String hour : map2.keySet()){
            hourSet.add(hour);
        }

        List<String> sortedList = new ArrayList<String>(hourSet);
        Map<String, Map<String, Integer>> reportingHourlyMap = new HashMap<String, Map<String, Integer>>();

        for(int i = sortedList.size() - 1; i >= 0 && sortedList.size() - i <= Constants.REPORTING_HOUR_WINDOW_LENGTH; i--){
            String dateInHour = sortedList.get(i);
            Map<String, Integer> hourlyMap = new HashMap<String, Integer>();
            int impressions = map1.containsKey(dateInHour) ? map1.get(dateInHour).get(Constants.IMPRESSIONS) : 0;
            int clicks = map1.containsKey(dateInHour) ? map1.get(dateInHour).get(Constants.CLICKS) : 0;
            int errors = map1.containsKey(dateInHour) ? map1.get(dateInHour).get(Constants.ERRORS) : 0;
            int conversions = map1.containsKey(dateInHour) ? map1.get(dateInHour).get(Constants.CONVERSIONS) : 0;

            if(map2.containsKey(dateInHour)){
                impressions += map2.get(dateInHour).get(Constants.IMPRESSIONS);
                clicks += map2.get(dateInHour).get(Constants.CLICKS);
                errors += map2.get(dateInHour).get(Constants.ERRORS);
                conversions += map2.get(dateInHour).get(Constants.CONVERSIONS);
            }

            hourlyMap.put(Constants.IMPRESSIONS, impressions);
            hourlyMap.put(Constants.CLICKS, clicks);
            hourlyMap.put(Constants.ERRORS, errors);
            hourlyMap.put(Constants.CONVERSIONS, conversions);

            reportingHourlyMap.put(dateInHour, hourlyMap);
        }

        return reportingHourlyMap;
    }
}
