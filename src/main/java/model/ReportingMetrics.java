package model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.PropertiesReader;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yiguo on 2/16/16.
 */
public class ReportingMetrics {
    private Map<String, Integer> reportingMetricsMap;

    private Logger logger = LoggerFactory.getLogger(getClass());
    public ReportingMetrics() {
        this.reportingMetricsMap = new HashMap<String, Integer>();
        for (String eventType : PropertiesReader.getAcceptedEventTypeList()){
            reportingMetricsMap.put(eventType, 0);
        }
    }

    public ReportingMetrics(String type, int count) {
        type = type.toLowerCase();
        this.reportingMetricsMap = new HashMap<String, Integer>();
        for (String eventType : PropertiesReader.getAcceptedEventTypeList()){
            reportingMetricsMap.put(eventType, 0);
        }
        reportingMetricsMap.put(type, count);
    }

    public ReportingMetrics(Map<String, Integer> reportingMetricsMap) {
        this.reportingMetricsMap = reportingMetricsMap;
    }

    public void setReportingMetricsMap(String typeStr, int count){
        if(PropertiesReader.isAcceptedEventType(typeStr)){
            this.reportingMetricsMap.put(typeStr, count);
        }
        else {
            logger.error("Unknown event type: " + typeStr);
        }
    }

    public Map<String, Integer> getReportingMetricsMap() {
        return reportingMetricsMap;
    }

    public ReportingMetrics clone() {
        ReportingMetrics clone = new ReportingMetrics();

        for(String metric : reportingMetricsMap.keySet()) {
            clone.setReportingMetricsMap(metric, reportingMetricsMap.get(metric));
        }

        return clone;
    }

    public void merge(ReportingMetrics anotherReportingMetrics) {
        Map<String, Integer> metricsMap = anotherReportingMetrics.getReportingMetricsMap();
        for(String metric : metricsMap.keySet()){
            if(reportingMetricsMap.containsKey(metric)){
                reportingMetricsMap.put(metric, reportingMetricsMap.get(metric) + metricsMap.get(metric));
            }
            else {
                reportingMetricsMap.put(metric, metricsMap.get(metric));
            }
        }
    }
}
