import backtype.storm.topology.TopologyBuilder;
import bolt.GenericAggregationBolt;
import spout.RandomRptMsgSpout;

/**
 * Created by Yiyong on 12/6/15.
 */
public class InGameMsgRptTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("spout", new RandomRptMsgSpout(), 5);

        topologyBuilder.setBolt("aggregate", new GenericAggregationBolt(), 5);

    }
}
