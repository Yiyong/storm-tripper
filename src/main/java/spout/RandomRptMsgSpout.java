package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import model.ReportingMessage;
import model.ReportingMessageSerializer;
import utils.Constants;
import utils.PropertiesReader;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by Yiyong on 11/29/15.
 */
public class RandomRptMsgSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    ReportingMessageSerializer reportingMessageSerializer = ReportingMessageSerializer.getInstance();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.EVENT));
        //declarer.declare(new Fields(Constants.AGGREGATE_KEY, Constants.IMPRESSIONS, Constants.CLICKS));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(10);
        String[] messageIDsSet = new String[]{"M1", "M2", "M3", "M4", "M5"};
        String[] contentIDsSet = new String[]{"C1", "C2", "C3", "C4"};
        String[] countrySet = new String[]{"US", "GE", "JP", "GB", "CN"};

//        ReportingMessage message = new ReportingMessage(messageIDsSet[_rand.nextInt(messageIDsSet.length)],
//                contentIDsSet[_rand.nextInt(contentIDsSet.length)],
//                countrySet[_rand.nextInt(countrySet.length)]);

        ReportingMessage message = new ReportingMessage(messageIDsSet[0],
                contentIDsSet[0],
                countrySet[0]);

        message.setImpressions(2);
        message.setClicks(1);
        message.setTimestamp(utils.Utils.getCurrentTimeStamp());

        _collector.emit(new Values(reportingMessageSerializer.serialize(message)));

        /*
        List<List<String>> aggregateFields = PropertiesReader.getAggregateFieldsList();
        for(List<String> aggregateField: aggregateFields){
            String aggregateKey = message.getAggregationKey(aggregateField);
            _collector.emit(new Values(aggregateKey, message.getImpressions(), message.getClicks()));
        }
        */
    }
}
