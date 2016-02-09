package trident.function;

/**
 * Created by yiguo on 1/19/16.
 */

import backtype.storm.tuple.Values;
import model.ReportingMessage;
import model.ReportingMessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import utils.PropertiesReader;

import java.util.List;

public class ReportEventsParser extends BaseFunction {

    private ReportingMessageSerializer reportingMessageSerializer = ReportingMessageSerializer.getInstance();
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        byte[] rawEvent = (byte[]) tuple.getValue(0);

        ReportingMessage message = reportingMessageSerializer.deserialize(rawEvent);

        if(message != null){
            List<List<String>> aggregateFields = PropertiesReader.getAggregateFieldsList();
            for(List<String> aggregateField: aggregateFields){
                String aggregateKey = message.getAggregationKey(aggregateField);
                collector.emit(new Values(aggregateKey, message.getType().toString(), message.getCount()));
            }
        }
        else {
            logger.error("Message is null.");
        }
    }
}
