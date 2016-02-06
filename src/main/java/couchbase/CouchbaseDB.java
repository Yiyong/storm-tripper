package couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import model.ReportingEventType;
import model.SimpleReportingMessage;
import rx.Observable;
import rx.functions.Func1;
import utils.Constants;
import utils.Utils;

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
                        return bucket.async().upsert(docToInsert);
                    }
                })
                .toList()
                .toBlocking()
                .single();
    }

/*    public List<SimpleReportingMessage> bulkGetSimpleReportingMessage(List<List<String>> keyAndTypes){
        List<String> keys = keyAndTypes.get(0);
        List<String> types = keyAndTypes.get(1);

        List<JsonDocument> jsonDocumentList = bulkGet(keys);
        List<SimpleReportingMessage> simpleReportingMessageList = new ArrayList<SimpleReportingMessage>();

        int i = 0;
        for(JsonDocument jsonDocument : jsonDocumentList){
            String aggregateKey = jsonDocument.id();
            while(!keys.get(i).equals(aggregateKey)){
                simpleReportingMessageList.add(null);
                i++;
            }
            ReportingEventType aggregateType = ReportingEventType.valueOf(types.get(i));
            String couchbaseStyleType = Utils.reportTypeToCouchbaseKey(aggregateType);
            if(jsonDocument.content().containsKey(couchbaseStyleType)){
                int count = jsonDocument.content().getInt(couchbaseStyleType);
                simpleReportingMessageList.add(new SimpleReportingMessage(aggregateKey, aggregateType, count));
            }
            else {
                simpleReportingMessageList.add(null);
            }
            i++;
        }

        while(i < keys.size()){
            simpleReportingMessageList.add(null);
            i++;
        }


        return simpleReportingMessageList;
    }*/

    public List<SimpleReportingMessage> bulkGetSimpleReportingMessage(List<String> keys) {
        List<JsonDocument> jsonDocumentList = bulkGet(keys);
        List<SimpleReportingMessage> simpleReportingMessageList = new ArrayList<SimpleReportingMessage>();

        int i = 0;
        for(JsonDocument jsonDocument : jsonDocumentList){
            String aggregateKey = jsonDocument.id();
            while(!keys.get(i).equals(aggregateKey)){
                simpleReportingMessageList.add(null);
                i++;
            }
            int impressions = jsonDocument.content().containsKey(Constants.IMPRESSIONS)?
                    jsonDocument.content().getInt(Constants.IMPRESSIONS) : 0;
            int clicks = jsonDocument.content().containsKey(Constants.CLICKS)?
                    jsonDocument.content().getInt(Constants.CLICKS) : 0;
            int errors = jsonDocument.content().containsKey(Constants.ERRORS)?
                    jsonDocument.content().getInt(Constants.ERRORS) : 0;
            int conversions = jsonDocument.content().containsKey(Constants.CONVERSIONS)?
                    jsonDocument.content().getInt(Constants.CONVERSIONS) : 0;
            simpleReportingMessageList.add(new SimpleReportingMessage(aggregateKey, impressions, clicks, errors, conversions));
            i++;
        }

        while(i < keys.size()){
            simpleReportingMessageList.add(null);
            i++;
        }


        return simpleReportingMessageList;
    }

    public void bulkPutSimpleReportingMessage(List<SimpleReportingMessage> simpleReportingMessages){
        List<JsonDocument> jsonDocumentList = new ArrayList<JsonDocument>();
        for(SimpleReportingMessage simpleReportingMessage : simpleReportingMessages){
            JsonObject reportsJson = JsonObject.empty()
                    .put(Constants.IMPRESSIONS, simpleReportingMessage.getImpressions())
                    .put(Constants.CLICKS, simpleReportingMessage.getClicks())
                    .put(Constants.ERRORS, simpleReportingMessage.getErrors())
                    .put(Constants.CONVERSIONS, simpleReportingMessage.getConversions());
            JsonDocument doc = JsonDocument.create(simpleReportingMessage.getAggregateKey(), reportsJson);
            jsonDocumentList.add(doc);
        }

        bulkPut(jsonDocumentList);
    }
}
