package couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import model.SimpleReportingMessage;
import rx.Observable;
import rx.functions.Func1;
import utils.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yiguo on 1/21/16.
 */
public class CouchbaseDB {
    private final Cluster couchbaseCluster;
    private Bucket bucket;

    private static CouchbaseDB couchbaseDB;

    private CouchbaseDB() {
        couchbaseCluster = CouchbaseCluster.create(Constants.COUCHBASE_CLUSTER);
        bucket = couchbaseCluster.openBucket(Constants.COUCHBASE_BUCKET_NAME, Constants.COUCHBASE_BUCKET_PASSWORD);
    }

    public static void init(){
        couchbaseDB = new CouchbaseDB();
    }

    public static CouchbaseDB getCouchbaseDB(){
        return couchbaseDB;
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

    public void bulkPut(List<JsonDocument> documents){
        Observable
                .from(documents)
                .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>(){
                    @Override
                    public Observable<JsonDocument> call(JsonDocument docToInsert) {
                        return bucket.async().insert(docToInsert);
                    }
                })
                .toList()
                .toBlocking()
                .single();
    }

    public List<SimpleReportingMessage> bulkGetSimpleReportingMessage(List<String> keys){
        List<JsonDocument> jsonDocumentList = bulkGet(keys);
        Map<String, SimpleReportingMessage> messageMap = new HashMap<String, SimpleReportingMessage>();
        for(JsonDocument jsonDocument : jsonDocumentList){
            messageMap.put(jsonDocument.id(), new SimpleReportingMessage(jsonDocument.id(),
                    jsonDocument.content().getInt(Constants.IMPRESSIONS),
                    jsonDocument.content().getInt(Constants.CLICKS)));
        }

        List<SimpleReportingMessage> simpleReportingMessageList = new ArrayList<SimpleReportingMessage>();

        for(String key : keys){
            if(messageMap.containsKey(key)){
                simpleReportingMessageList.add(messageMap.get(key));
            }
            else simpleReportingMessageList.add(null);
        }

        return simpleReportingMessageList;
    }

    public void bulkPutSimpleReportingMessage(List<SimpleReportingMessage> simpleReportingMessages){
        List<JsonDocument> jsonDocumentList = new ArrayList<JsonDocument>();
        for(SimpleReportingMessage simpleReportingMessage : simpleReportingMessages){
            JsonObject reportsJson = JsonObject.empty()
                    .put(Constants.IMPRESSIONS, simpleReportingMessage.getImpressions())
                    .put(Constants.CLICKS, simpleReportingMessage.getClicks());

            JsonDocument doc = JsonDocument.create(simpleReportingMessage.getAggregateKey(), reportsJson);
            jsonDocumentList.add(doc);
        }

        bulkPut(jsonDocumentList);
    }
}
