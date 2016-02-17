package trident.aggregator;

import model.ReportingMetrics;
import model.SimpleReportingMessage;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

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

        return new SimpleReportingMessage(aggregateKey, dateInHour, new ReportingMetrics(type, count));
    }

    @Override
    public SimpleReportingMessage combine(SimpleReportingMessage val1, SimpleReportingMessage val2) {
        val1.mergeWithAnotherReportingMessage(val2);
        return val1;
    }

    @Override
    public SimpleReportingMessage zero() {
        return null;
    }
}
