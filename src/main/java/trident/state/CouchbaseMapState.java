package trident.state;

import backtype.storm.task.IMetricsContext;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.document.JsonDocument;
import rx.Observable;
import rx.functions.Func1;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.TransactionalMap;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yiguo on 1/20/16.
 */
public class CouchbaseMapState<SimpleReportingMessage> implements IBackingMap<SimpleReportingMessage> {

    private Cluster couchbaseCluster;
    private Bucket bucket;
    private Serializer serializer;

    public static StateFactory transactional(List<URI> uris, String bucket,
                                             String password, List<String> tagFields){

    }

    public static StateFactory FACTORY = new StateFactory() {
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return TransactionalMap.build(new CouchbaseMapState());
        }
    };

    private List<String> getKeys(List<List<Object>> keys){
        List<String> aggregateKeys = new ArrayList<String>();
        for(List<Object> object : keys){
            aggregateKeys.add((String) object.get(0));
        }

        return aggregateKeys;
    }

    private List<JsonDocument> bulkGet(List<String> keys){
        return Observable
                .from(keys)
                .flatMap(new Func1<String, Observable<JsonDocument>>(){
                    @Override
                    public Observable<JsonDocument> call(String key) {
                        return bucket.async().get(key);
                    }
                })
                .toList()
                .toBlocking()
                .single();
    }

    @Override
    public List<SimpleReportingMessage> multiGet(List<List<Object>> keys) {
        List<String> aggregateKeys = getKeys(keys);
        List<SimpleReportingMessage> ret = new ArrayList<SimpleReportingMessage>();
        List<JsonDocument> values = bulkGet(aggregateKeys);
        for (int i = 0; i < aggregateKeys.size(); i++)
        {
            Object value = values.get(i);
            if (value != null)
            {
                SimpleReportingMessage message = (SimpleReportingMessage) serializer.deserialize((byte[]) value);
                ret.add(message);
            } else
            {
                ret.add(null);
            }
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<SimpleReportingMessage> vals) {

    }
}
