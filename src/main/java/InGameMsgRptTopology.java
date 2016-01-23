//import backtype.storm.Config;
//import backtype.storm.LocalCluster;
//import backtype.storm.StormSubmitter;
//import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.tuple.Fields;
//import backtype.storm.utils.Utils;
//import bolt.GenericAggregationBolt;
//import bolt.IntermediateBolt;
//import spout.RandomRptMsgSpout;
//
///**
// * Created by Yiyong on 12/6/15.
// */
//public class InGameMsgRptTopology {
//    public static void main(String[] args) throws Exception {
//        TopologyBuilder topologyBuilder = new TopologyBuilder();
//
//        topologyBuilder.setSpout("spout", new RandomRptMsgSpout(), 5);
//
//        topologyBuilder.setBolt("intermediate", new IntermediateBolt(), 5).shuffleGrouping("spout");
//
//        topologyBuilder.setBolt("aggregate", new GenericAggregationBolt(), 5).fieldsGrouping("intermediate", new Fields("message"));
//
//        Config conf = new Config();
//        conf.setDebug(false);
//
//        if (args != null && args.length > 0) {
//            conf.setNumWorkers(3);
//
//            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topologyBuilder.createTopology());
//        }
//        else {
//            conf.setMaxTaskParallelism(3);
//
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("InGameMsgRpt", conf, topologyBuilder.createTopology());
//
//            Utils.sleep(200000);
//
//            cluster.killTopology("InGameMsgRpt");
//            cluster.shutdown();
//        }
//    }
//}
