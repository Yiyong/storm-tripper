package trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import couchbase.CouchbaseDB;
import spout.RandomRptMsgSpout;
import storm.trident.TridentTopology;
import storm.trident.spout.RichSpoutBatchExecutor;
import trident.aggregator.GenericHourlyCounter;
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
                        new Fields(Constants.AGGREGATE_KEY, Constants.TYPE, Constants.COUNT))
                .shuffle()
                .project(new Fields(Constants.AGGREGATE_KEY, Constants.TYPE, Constants.COUNT))
                .groupBy(new Fields(Constants.AGGREGATE_KEY))
                .persistentAggregate(
                        CouchbaseMapState.FACTORY,
                        new Fields(Constants.AGGREGATE_KEY, Constants.TYPE, Constants.COUNT),
                        new GenericHourlyCounter(),
                        new Fields(Constants.GENERIC_REPORTING)).parallelismHint(5);

        return topology.build();
    }

    public static void main(String[] args) {
        PropertiesReader.loadProperties();
        CouchbaseDB.init();

        EASpoutConfigurer eaSpoutConfigurer = new EASpoutConfigurer();
        TopologyBuilder tb = buildTopology(eaSpoutConfigurer);

        Config conf = new Config();
        conf.setMaxSpoutPending(PropertiesReader.getMaxSpoutPending());
        conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, PropertiesReader.getMaxBatchSize());

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("aggregateReporter", conf, buildTopology());
    }
}
