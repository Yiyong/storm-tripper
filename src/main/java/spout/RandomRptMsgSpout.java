package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import model.ReportingMessage;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by Yiyong on 11/29/15.
 */
public class RandomRptMsgSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    List<String> aggregateFields;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        aggregateFields = new LinkedList<String>();

        for(Object key : conf.keySet()){
            aggregateFields.add((String) key);
        }
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String[] messageIDsSet = new String[]{"M1", "M2", "M3", "M4", "M5"};
        String[] contentIDsSet = new String[]{"C1", "C2", "C3", "C4"};
        String[] countrySet = new String[]{"US", "GE", "JP", "GB", "CN"};

        ReportingMessage message = new ReportingMessage(messageIDsSet[_rand.nextInt(messageIDsSet.length)],
                contentIDsSet[_rand.nextInt(contentIDsSet.length)],
                countrySet[_rand.nextInt(countrySet.length)]);

        for(String aggregateField: aggregateFields){
            String aggregateKey = message.getAggregationKey();
        }

        _collector.emit(new Values(message));
    }
}
