package trident.aggregator;

import model.ReportingEventType;
import model.SimpleReportingMessage;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by yiguo on 1/19/16.
 */
public class GenericReportingHourlyCounter implements CombinerAggregator<SimpleReportingMessage> {

    @Override
    public SimpleReportingMessage init(TridentTuple tuple) {
        String aggregateKey = tuple.getString(0);
        String dateInHour = tuple.getString(1);
        String type = tuple.getString(2);
        int count = tuple.getInteger(3);

        return new SimpleReportingMessage(aggregateKey, dateInHour, ReportingEventType.valueOf(type), count);
    }

    @Override
    public SimpleReportingMessage combine(SimpleReportingMessage val1, SimpleReportingMessage val2) {
        Map<String, Map<String, Integer>> hourlyReportingMap1 = val1.getHourlyReportingMap();
        Map<String, Map<String, Integer>> hourlyReportingMap2 = val2.getHourlyReportingMap();
        Map<String, Map<String, Integer>> mergedHourlyReportingMap = SimpleReportingMessage.mergeHourlyReportingMap(hourlyReportingMap1, hourlyReportingMap2);

        return new SimpleReportingMessage(val1.getAggregateKey(),
                val1.getImpressions() + val2.getImpressions(),
                val1.getClicks() + val2.getClicks(),
                val1.getErrors() + val2.getErrors(),
                val1.getConversions() + val2.getConversions(),
                mergedHourlyReportingMap);
    }

    @Override
    public SimpleReportingMessage zero() {
        return null;
    }
}
