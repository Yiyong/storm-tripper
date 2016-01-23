package trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import spout.RandomRptMsgSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import trident.aggregator.GenericHourlyCounter;
import trident.function.ReportEventsParser;
import trident.state.CouchbaseMapState;
import utils.Constants;

/**
 * Created by yiguo on 1/19/16.
 */
public class InGameMsgRptTrdTopology {
    public static StormTopology buildTopology(){
        TridentTopology topology = new TridentTopology();
        Stream reportingEventsStream = topology.
                newStream("spout", new RandomRptMsgSpout())
                .parallelismHint(5)
                .each(new Fields(Constants.EVENT),
                        new ReportEventsParser(),
                        new Fields(Constants.AGGREGATE_KEY, Constants.IMPRESSIONS, Constants.CLICKS));

        genericReportingEventsProcessing(reportingEventsStream);
        return topology.build();
    }

    private static void genericReportingEventsProcessing(Stream stream){
        stream.groupBy(new Fields(Constants.AGGREGATE_KEY))
                .persistentAggregate(CouchbaseMapState.FACTORY,
                        new Fields(Constants.AGGREGATE_KEY, Constants.IMPRESSIONS, Constants.CLICKS),
                        new GenericHourlyCounter(),
                        new Fields(Constants.GENERIC_REPORTING));
    }

    public static void main(String[] args) {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("aggregateReporter", conf, buildTopology());
    }
}
