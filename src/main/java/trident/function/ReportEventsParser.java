package trident.function;

/**
 * Created by yiguo on 1/19/16.
 */

import backtype.storm.tuple.Values;
import model.ReportingMessage;
import model.ReportingMessageSerializer;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import utils.PropertiesReader;

import java.util.List;

public class ReportEventsParser extends BaseFunction {

    private ReportingMessageSerializer reportingMessageSerializer = ReportingMessageSerializer.getInstance();

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        byte[] rawEvent = (byte[]) tuple.getValue(0);

        ReportingMessage message = reportingMessageSerializer.deserialize(rawEvent);

        if(message != null){
            List<List<String>> aggregateFields = PropertiesReader.getAggregateFieldsList();
            for(List<String> aggregateField: aggregateFields){
                String aggregateKey = message.getAggregationKey(aggregateField);
                collector.emit(new Values(aggregateKey, message.getImpressions(), message.getClicks()));
            }
        }
        else System.err.println("Reporting message is null.");
    }
}
