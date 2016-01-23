package trident.state;

import backtype.storm.task.IMetricsContext;
import couchbase.CouchbaseDB;
import model.SimpleReportingMessage;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yiguo on 1/20/16.
 */
public class CouchbaseMapState implements IBackingMap<SimpleReportingMessage> {

    private CouchbaseDB couchbaseDB;

    public static StateFactory FACTORY = new StateFactory() {
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return NonTransactionalMap.build(new CouchbaseMapState());
        }
    };

    protected CouchbaseMapState(){
        couchbaseDB = CouchbaseDB.getCouchbaseDB();
    }

    private List<String> getKeys(List<List<Object>> keys){
        List<String> aggregateKeys = new ArrayList<String>();
        for(List<Object> object : keys){
            aggregateKeys.add((String) object.get(0));
        }

        return aggregateKeys;
    }

    @Override
    public List<SimpleReportingMessage> multiGet(List<List<Object>> keys) {
        List<String> aggregateKeys = getKeys(keys);
        List<SimpleReportingMessage> values = couchbaseDB.bulkGetSimpleReportingMessage(aggregateKeys);
        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<SimpleReportingMessage> simpleReportingMessages) {
        couchbaseDB.bulkPutSimpleReportingMessage(simpleReportingMessages);
    }
}
