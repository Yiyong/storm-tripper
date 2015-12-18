package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
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
        String aggregateKey = input.getString(0);
        int impressions = input.getInteger(1);
        int clicks = input.getInteger(2);

        int[] reports = counts.get(aggregateKey);

        if(reports == null){
            reports = new int[2];
            reports[0] = impressions;
            reports[1] = clicks;
            counts.put(aggregateKey, reports);
        }
        else {
            System.err.println("key: " + aggregateKey + "  impressions: " + reports[0] + " clicks: " + reports[1]);
            reports[0] += impressions;
            reports[1] += clicks;
            System.err.println("key: " + aggregateKey + "  impressions: " + reports[0] + " clicks: " + reports[1]);
        }

        _collector.emit(new Values(aggregateKey, reports[0], reports[1]));
        System.err.println("[FINAL] key: " + aggregateKey + "  impressions: " + reports[0] + " clicks: " + reports[1]);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "impressions", "clicks"));
    }
}
