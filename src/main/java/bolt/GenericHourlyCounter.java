package bolt;

import model.SimpleReportingMessage;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * Created by yiguo on 1/19/16.
 */
public class GenericHourlyCounter implements CombinerAggregator<SimpleReportingMessage> {

    @Override
    public SimpleReportingMessage init(TridentTuple tuple) {
        String aggregateKey = tuple.getString(0);
        int impressions = tuple.getInteger(1);
        int clicks = tuple.getInteger(2);

        return new SimpleReportingMessage(aggregateKey, impressions, clicks);
    }

    @Override
    public SimpleReportingMessage combine(SimpleReportingMessage val1, SimpleReportingMessage val2) {
        return new SimpleReportingMessage(val1.getAggregateKey(),
                val1.getImpressions() + val2.getImpressions(),
                val1.getClicks() + val2.getClicks());
    }

    @Override
    public SimpleReportingMessage zero() {
        return null;
    }
}
