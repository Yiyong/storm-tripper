import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolt.GenericAggregationBolt;
import spout.RandomRptMsgSpout;

/**
 * Created by Yiyong on 12/6/15.
 */
public class InGameMsgRptTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("spout", new RandomRptMsgSpout(), 5);

        topologyBuilder.setBolt("aggregate", new GenericAggregationBolt(), 5).fieldsGrouping("spout", new Fields("message"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topologyBuilder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("poc", conf, topologyBuilder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}
