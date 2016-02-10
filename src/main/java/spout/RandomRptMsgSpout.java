package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import model.ReportingMessage;
import model.ReportingMessageSerializer;
import utils.Constants;
import utils.Utils;

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
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        backtype.storm.utils.Utils.sleep(100);
        String[] messageIDsSet = new String[]{"M1", "M2", "M3", "M4", "M5"};
        String[] contentIDsSet = new String[]{"C1", "C2", "C3", "C4"};
        String[] destinationIDsSet = new String[]{"D1", "D2", "D3", "D4", "D5"};
        String[] destinationIdentifierSet = new String[]{"DI1", "DI2", "DI3", "DI4", "DI5"};
        String[] segmentIdSet = new String[]{"S1", "S2"};
        String[] countrySet = new String[]{"US", "GE", "JP", "GB", "CN"};
        String[] citySet = new String[]{"Xian", "Redwood Shores", "Tokyo", "Mountain View", "Sunnyvale"};
        String[] typeSet = new String[]{"IMPRESSION", "CLICK", "ERROR", "CONVERSION"};

        String messageId = messageIDsSet[_rand.nextInt(messageIDsSet.length)];
        String contentId = contentIDsSet[_rand.nextInt(contentIDsSet.length)];
        String destinationId = destinationIDsSet[_rand.nextInt(destinationIDsSet.length)];
        String destinationIdentifier = destinationIdentifierSet[_rand.nextInt(destinationIdentifierSet.length)];
        String segmentId = segmentIdSet[_rand.nextInt(segmentIdSet.length)];
        String country = countrySet[_rand.nextInt(countrySet.length)];
        String city = citySet[_rand.nextInt(citySet.length)];
        String type = typeSet[_rand.nextInt(typeSet.length)];

        ReportingMessage message = new ReportingMessage(messageId, destinationId, destinationIdentifier, segmentId, contentId, country, city);

        message.setTimestamp(Utils.getCurrentTimeStamp());
        message.setType(type);
        message.setCount(_rand.nextInt(20));

        _collector.emit(new Values(reportingMessageSerializer.serialize(message)));
    }

}
