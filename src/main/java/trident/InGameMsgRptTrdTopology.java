package trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import couchbase.CouchbaseDB;
import spout.RandomRptMsgSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import trident.aggregator.GenericHourlyCounter;
import trident.aggregator.GenericLifetimeAggregator;
import trident.function.ReportEventsParser;
import trident.state.CouchbaseMapState;
import utils.Constants;
import utils.PropertiesReader;
import utils.StreamPrinter;

/**
 * Created by yiguo on 1/19/16.
 */
public class InGameMsgRptTrdTopology {
    public static StormTopology buildTopology(LocalDRPC drpc){
        TridentTopology topology = new TridentTopology();

        TridentState reports = topology.newStream("spout", new RandomRptMsgSpout())
                .parallelismHint(5)
                .each(new Fields(Constants.EVENT),
                        new ReportEventsParser(),
                        new Fields(Constants.AGGREGATE_KEY, Constants.IMPRESSIONS, Constants.CLICKS))
                .shuffle()
                .project(new Fields(Constants.AGGREGATE_KEY, Constants.IMPRESSIONS, Constants.CLICKS))
                .groupBy(new Fields(Constants.AGGREGATE_KEY))
                .persistentAggregate(
                        new MemoryMapState.Factory(),
                        new GenericLifetimeAggregator(),
                        new Fields(Constants.GENERIC_REPORTING)).parallelismHint(5);


        topology.newDRPCStream("key", drpc)
                .each(new Fields("args"), new Split(), new Fields(Constants.AGGREGATE_KEY))
                .stateQuery(reports, new Fields(Constants.AGGREGATE_KEY), new MapGet(), new Fields("reports"));

        return topology.build();
    }

    public static void main(String[] args) {
        PropertiesReader propertiesReader = PropertiesReader.getPropertiesReader();
        //CouchbaseDB.init();

        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("aggregateReporter", conf, buildTopology(drpc));
        for (int i = 0; i < 10; i++) {
            System.err.println("DRPC RESULT: " + drpc.execute("key", "msgM1"));
            Utils.sleep(1000);
        }
        Utils.sleep(60000);

        cluster.killTopology("aggregateReporter");
        cluster.shutdown();
    }
}
