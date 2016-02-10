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

    public static void init() {
        couchbaseDB = new CouchbaseDB();
    }

    public static CouchbaseDB getCouchbaseDB() {
        return couchbaseDB;
    }

    private List<JsonDocument> bulkGet(List<String> keys) {
        return Observable
                .from(keys)
                .flatMap(new Func1<String, Observable<JsonDocument>>() {
                    @Override
                    public Observable<JsonDocument> call(String key) {
                        return bucket.async().get(key);
                    }
                })
                .toList()
                .toBlocking()
                .single();
    }

    public void bulkPut(List<JsonDocument> documents) {
        Observable
                .from(documents)
                .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                    @Override
                    public Observable<JsonDocument> call(JsonDocument docToInsert) {
                        return bucket.async().upsert(docToInsert);
                    }
                })
                .toList()
                .toBlocking()
                .single();
    }

    public List<SimpleReportingMessage> bulkGetSimpleReportingMessage(List<String> keys) {
        List<JsonDocument> jsonDocumentList = bulkGet(keys);
        List<SimpleReportingMessage> simpleReportingMessageList = new ArrayList<SimpleReportingMessage>();

        int i = 0;
        for (JsonDocument jsonDocument : jsonDocumentList) {
            String aggregateKey = jsonDocument.id();
            while (!keys.get(i).equals(aggregateKey)) {
                simpleReportingMessageList.add(null);
                i++;
            }

            JsonObject content = jsonDocument.content();

            int impressions = 0;
            int clicks = 0;
            int errors = 0;
            int conversions = 0;

            Map<String, Map<String, Object>> hourlyReportingMap = new HashMap<String, Map<String, Object>>();

            for (String key : content.getNames()) {
                if (key.equals(Constants.IMPRESSIONS)) {
                    impressions = content.getInt(key);
                } else if (key.equals(Constants.CLICKS)) {
                    clicks = content.getInt(key);
                } else if (key.equals(Constants.ERRORS)) {
                    errors = content.getInt(key);
                } else if (key.equals(Constants.CONVERSIONS)) {
                    conversions = content.getInt(key);
                } else if (Utils.isValidDateInHour(key)) {
                    hourlyReportingMap.put(key, content.getObject(key).toMap());
                }
            }

            simpleReportingMessageList.add(new SimpleReportingMessage(aggregateKey, "", impressions, clicks, errors, conversions));
            i++;
        }

        while (i < keys.size()) {
            simpleReportingMessageList.add(null);
            i++;
        }

        return simpleReportingMessageList;
    }

    public void bulkPutSimpleReportingMessage(List<SimpleReportingMessage> simpleReportingMessages) {
        List<JsonDocument> jsonDocumentList = new ArrayList<JsonDocument>();
        for (SimpleReportingMessage simpleReportingMessage : simpleReportingMessages) {
            JsonObject reportsJson = JsonObject.empty()
                    .put(Constants.IMPRESSIONS, simpleReportingMessage.getImpressions())
                    .put(Constants.CLICKS, simpleReportingMessage.getClicks())
                    .put(Constants.ERRORS, simpleReportingMessage.getErrors())
                    .put(Constants.CONVERSIONS, simpleReportingMessage.getConversions());

            Map<String, Map<String, Integer>> hourlyReportingMap = simpleReportingMessage.getHourlyReportingMap();
            for (String dateInHour : hourlyReportingMap.keySet()) {
                reportsJson.put(dateInHour, hourlyReportingMap.get(dateInHour));
            }

            JsonDocument doc = JsonDocument.create(simpleReportingMessage.getAggregateKey(), reportsJson);
            jsonDocumentList.add(doc);
        }

        bulkPut(jsonDocumentList);
    }
}
