package couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import model.ReportingMetrics;
import model.SimpleReportingMessage;
import rx.Observable;
import rx.functions.Func1;
import utils.Constants;
import utils.PropertiesReader;
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
        Map<String, JsonDocument> jsonDocumentMap = new HashMap<String, JsonDocument>();

        for (JsonDocument jsonDocument : jsonDocumentList) {
            jsonDocumentMap.put(jsonDocument.id(), jsonDocument);
        }

        List<SimpleReportingMessage> simpleReportingMessageList = new ArrayList<SimpleReportingMessage>();

        for (String aggregateKey : keys) {
            if (!jsonDocumentMap.containsKey(aggregateKey)) {
                simpleReportingMessageList.add(null);
            } else {
                JsonObject content = jsonDocumentMap.get(aggregateKey).content();
                Map<String, ReportingMetrics> hourlyReportingMap = new HashMap<String, ReportingMetrics>();
                ReportingMetrics reportingMetrics = new ReportingMetrics();

                for (String key : content.getNames()) {
                    if (PropertiesReader.isAcceptedEventType(key)) {
                        reportingMetrics.setReportingMetricsMap(key, content.getInt(key));
                    } else if (Utils.isValidDateInHour(key)) {
                        Map<String, Object> oneHourReportingRawMap = content.getObject(key).toMap();
                        Map<String, Integer> oneHourReportingMap = new HashMap<String, Integer>();

                        for (String metrics : oneHourReportingRawMap.keySet()) {
                            if (PropertiesReader.isAcceptedEventType(metrics)) {
                                oneHourReportingMap.put(metrics, (Integer) oneHourReportingRawMap.get(metrics));
                            }
                        }
                        hourlyReportingMap.put(key, new ReportingMetrics(oneHourReportingMap));
                    }
                }
                simpleReportingMessageList.add(new SimpleReportingMessage(aggregateKey, reportingMetrics, hourlyReportingMap));
            }
        }

        return simpleReportingMessageList;
    }

    public void bulkPutSimpleReportingMessage(List<SimpleReportingMessage> simpleReportingMessages) {
        List<JsonDocument> jsonDocumentList = new ArrayList<JsonDocument>();
        for (SimpleReportingMessage simpleReportingMessage : simpleReportingMessages) {
            JsonObject reportsJson = JsonObject.empty();
            Map<String, Integer> reportingMetricsMap = simpleReportingMessage.getReportingMetrics().getReportingMetricsMap();
            for (String metric : reportingMetricsMap.keySet()) {
                reportsJson.put(metric, reportingMetricsMap.get(metric));
            }

            Map<String, ReportingMetrics> hourlyReportingMap = simpleReportingMessage.getHourlyReportingMap();
            for (String dateInHour : hourlyReportingMap.keySet()) {
                reportsJson.put(dateInHour, hourlyReportingMap.get(dateInHour).getReportingMetricsMap());
            }

            JsonDocument doc = JsonDocument.create(simpleReportingMessage.getAggregateKey(), reportsJson);
            jsonDocumentList.add(doc);
        }

        bulkPut(jsonDocumentList);
    }
}
