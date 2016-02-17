package model;

import utils.Constants;

import java.util.*;

/**
 * Created by yiguo on 1/20/16.
 */
public class SimpleReportingMessage {
    private String aggregateKey;
    private ReportingMetrics reportingMetrics;
    private Map<String, ReportingMetrics> hourlyReportingMap;

    public SimpleReportingMessage(String aggregateKey, ReportingMetrics reportingMetrics, Map<String, ReportingMetrics> hourlyReportingMap) {
        this.aggregateKey = aggregateKey;
        this.reportingMetrics = reportingMetrics;
        this.hourlyReportingMap = hourlyReportingMap;
    }

    public SimpleReportingMessage(String aggregateKey, String dateInHour, ReportingMetrics reportingMetrics) {
        this.aggregateKey = aggregateKey;
        this.reportingMetrics = reportingMetrics;

        initialHourlyReportingMap(dateInHour);
    }

    public String getAggregateKey() {
        return aggregateKey;
    }

    public void setAggregateKey(String aggregateKey) {
        this.aggregateKey = aggregateKey;
    }

    public ReportingMetrics getReportingMetrics() {
        return reportingMetrics;
    }

    public void setReportingMetrics(ReportingMetrics reportingMetrics) {
        this.reportingMetrics = reportingMetrics;
    }

    private void initialHourlyReportingMap(String dateInHour) {
        hourlyReportingMap = new HashMap<String, ReportingMetrics>();
        ReportingMetrics oneHourReportingMetrics = this.reportingMetrics.clone();

        hourlyReportingMap.put(dateInHour, oneHourReportingMetrics);
    }

    public Map<String, ReportingMetrics> getHourlyReportingMap() {
        return hourlyReportingMap;
    }

    private void setHourlyReportingMap(Map<String, ReportingMetrics> hourlyReportingMap) {
        this.hourlyReportingMap = hourlyReportingMap;
    }

    public void mergeWithAnotherReportingMessage(SimpleReportingMessage anotherSimpleReportingMessage) {
        this.reportingMetrics.merge(anotherSimpleReportingMessage.getReportingMetrics());
        mergeHourlyReportingMap(hourlyReportingMap, anotherSimpleReportingMessage.getHourlyReportingMap());
    }

    private void mergeHourlyReportingMap(Map<String, ReportingMetrics> map1, Map<String, ReportingMetrics> map2) {
        Set<String> hourSet = new HashSet<String>();
        for (String hour : map1.keySet()) {
            hourSet.add(hour);
        }
        for (String hour : map2.keySet()) {
            hourSet.add(hour);
        }

        List<String> sortedList = new ArrayList<String>(hourSet);
        Collections.sort(sortedList);

        Map<String, ReportingMetrics> reportingHourlyMap = new HashMap<String, ReportingMetrics>();
        for (int i = sortedList.size() - 1; i >= 0 && sortedList.size() - i <= Constants.REPORTING_HOUR_WINDOW_LENGTH; i--) {
            String dateInHour = sortedList.get(i);
            if (map1.containsKey(dateInHour) && map2.containsKey(dateInHour)) {
                map1.get(dateInHour).merge(map2.get(dateInHour));
                reportingHourlyMap.put(dateInHour, map1.get(dateInHour));
            } else if (map1.containsKey(dateInHour)) {
                reportingHourlyMap.put(dateInHour, map1.get(dateInHour));
            } else if (map2.containsKey(dateInHour)) {
                reportingHourlyMap.put(dateInHour, map2.get(dateInHour));
            }
        }

        this.setHourlyReportingMap(reportingHourlyMap);
    }
}
