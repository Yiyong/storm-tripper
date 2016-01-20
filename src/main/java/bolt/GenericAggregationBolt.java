package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Yiyong on 12/6/15.
 */
public class GenericAggregationBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private Cluster couchbaseCluster;
    private Bucket bucket;
    Map<String, int[]> counts = new HashMap<String, int[]>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        couchbaseCluster = CouchbaseCluster.create("*");
        bucket = couchbaseCluster.openBucket("aggregate_reporting", "*");
    }

    @Override
    public void execute(Tuple input) {
        String aggregateKey = input.getString(0);
        int impressions = input.getInteger(1);
        int clicks = input.getInteger(2);

        int[] reports = counts.get(aggregateKey);

        if(reports == null){
            reports = new int[2];
            reports[0] = impressions;
            reports[1] = clicks;
            counts.put(aggregateKey, reports);
        }
        else {
            reports[0] += impressions;
            reports[1] += clicks;
        }

        JsonObject reportsJson = JsonObject.empty()
                .put("impressions", reports[0])
                .put("clicks", reports[1]);

        JsonDocument doc = JsonDocument.create(aggregateKey, reportsJson);
        JsonDocument response = bucket.upsert(doc);

        System.err.println("[FINAL] key: " + aggregateKey + "  impressions: " + reports[0] + " clicks: " + reports[1]);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message", "impressions", "clicks"));
    }
}
