package utils;

/**
 * Created by yiguo on 12/15/15.
 */
public class Constants {
    public static final String EVENT = "event";
    public static final String AGGREGATE_KEY = "aggregateKey";
    public static final String TYPE = "type";
    public static final String COUNT = "count";
    public static final String IMPRESSIONS = "impressions";
    public static final String CLICKS = "clicks";
    public static final String ERRORS = "errors";
    public static final String CONVERSIONS = "conversions";
    public static final String GENERIC_REPORTING = "generic_reporting";
    public static final String DELIMITER = "$";

    public static final String COUCHBASE_CLUSTER = "10.102.74.163";
    public static final String COUCHBASE_BUCKET_NAME = "aggregate_reporting";
    public static final String COUCHBASE_BUCKET_PASSWORD = "eaprm$123";

    public static final String CONFIG = "conf";
    public static final String YAML_CONFIG_PATH = CONFIG + "/" + "storm-tripper.yaml";
}
