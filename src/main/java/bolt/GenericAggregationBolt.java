package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import model.ReportingMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Yiyong on 12/6/15.
 */
public class GenericAggregationBolt extends BaseRichBolt {
    private OutputCollector _collector;
    Map<String, int[]> counts = new HashMap<String, int[]>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        ReportingMessage message = (ReportingMessage) input.getValue(0);
        int impressions = message.getImpressions();
        int clicks = message.getClicks();

        int[] reports = counts.get(message.getAggregationKey());
        if(reports == null){
            reports = new int[2];
            reports[0] = impressions;
            reports[1] = clicks;
            counts.put(message.getAggregationKey(), reports);
        }
        else {
            reports[0] += impressions;
            reports[1] += clicks;
        }

        _collector.emit(new Values(message.getAggregationKey(), reports[0], reports[1]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
