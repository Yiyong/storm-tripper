package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import model.ReportingMessage;
import utils.PropertiesReader;

import java.util.*;

/**
 * Created by Yiyong on 11/29/15.
 */
public class RandomRptMsgSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "impressions", "clicks"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);
        String[] messageIDsSet = new String[]{"M1", "M2", "M3", "M4", "M5"};
        String[] contentIDsSet = new String[]{"C1", "C2", "C3", "C4"};
        String[] countrySet = new String[]{"US", "GE", "JP", "GB", "CN"};

//        ReportingMessage message = new ReportingMessage(messageIDsSet[_rand.nextInt(messageIDsSet.length)],
//                contentIDsSet[_rand.nextInt(contentIDsSet.length)],
//                countrySet[_rand.nextInt(countrySet.length)]);

        ReportingMessage message = new ReportingMessage(messageIDsSet[0],
                contentIDsSet[1],
                countrySet[2]);

        message.setImpressions(_rand.nextInt(100));
        message.setClicks(_rand.nextInt(10));

        List<List<String>> aggregateFields = PropertiesReader.getAggregateFieldsList();
        for(List<String> aggregateField: aggregateFields){
            String aggregateKey = message.getAggregationKey(aggregateField);
            System.out.println(aggregateField + " " + message.getImpressions() + " " + message.getClicks());
            _collector.emit(new Values(aggregateKey, message.getImpressions(), message.getClicks()));
        }
    }
}
