package trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import bolt.GenericHourlyCounter;
import trident.function.ReportEventsParser;
import spout.RandomRptMsgSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.testing.MemoryMapState;
import utils.Constants;

/**
 * Created by yiguo on 1/19/16.
 */
public class InGameMsgRptTrdTopology {
    public static StormTopology buildTopology(LocalDRPC drpc){
        TridentTopology topology = new TridentTopology();
        Stream reportingEventsStream = topology.
                newStream("spout", new RandomRptMsgSpout())
                .parallelismHint(5)
                .each(new Fields(Constants.EVENT),
                        new ReportEventsParser(),
                        new Fields(Constants.AGGREGATE_KEY, Constants.IMPRESSION, Constants.CLICK));

        genericReportingEventsProcessing(reportingEventsStream);
        return topology.build();
    }

    private static void genericReportingEventsProcessing(Stream stream){
        stream.groupBy(new Fields(Constants.AGGREGATE_KEY))
                .persistentAggregate(new MemoryMapState.Factory(), new GenericHourlyCounter(), new Fields());
    }

    public static void main(String[] args) {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);

        if(args.length == 0){
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("aggregateReporter", conf, buildTopology(drpc));
            for(int i = 0; i < 100; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("aggregatedKey", "JP"));
            }
        }
    }
}
