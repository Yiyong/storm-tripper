package trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import couchbase.CouchbaseDB;
import spout.RandomRptMsgSpout;
import storm.trident.TridentTopology;
import storm.trident.testing.MemoryMapState;
import trident.aggregator.GenericHourlyCounter;
import trident.aggregator.GenericLifetimeAggregator;
import trident.function.ReportEventsParser;
import trident.state.CouchbaseMapState;
import utils.Constants;
import utils.PropertiesReader;

/**
 * Created by yiguo on 1/19/16.
 */
public class InGameMsgRptTrdTopology {
    public static StormTopology buildTopology(){
        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", new RandomRptMsgSpout())
                .parallelismHint(5)
                .each(new Fields(Constants.EVENT),
                        new ReportEventsParser(),
                        new Fields(Constants.AGGREGATE_KEY, Constants.IMPRESSIONS, Constants.CLICKS))

                .groupBy(new Fields(Constants.AGGREGATE_KEY))
                .persistentAggregate(CouchbaseMapState.FACTORY,
                        new Fields(Constants.AGGREGATE_KEY, Constants.IMPRESSIONS, Constants.CLICKS),
                        new GenericHourlyCounter(),
                        new Fields(Constants.GENERIC_REPORTING));

//                .groupBy(new Fields(Constants.AGGREGATE_KEY))
//                .persistentAggregate(new MemoryMapState.Factory(),
//                        new GenericHourlyCounter(),
//                        new Fields(Constants.GENERIC_REPORTING));

        return topology.build();
    }

    public static void main(String[] args) {
        PropertiesReader propertiesReader = PropertiesReader.getPropertiesReader();
        CouchbaseDB.init();

        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("aggregateReporter", conf, buildTopology());
        Utils.sleep(60000);

        cluster.killTopology("aggregateReporter");
        cluster.shutdown();
    }
}
