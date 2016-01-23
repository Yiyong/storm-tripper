package utils;

/**
 * Created by yiguo on 12/15/15.
 */
public class Constants {
    public static final String EVENT = "event";
    public static final String AGGREGATE_KEY = "aggregateKey";
    public static final String IMPRESSIONS = "impressions";
    public static final String CLICKS = "clicks";
    public static final String GENERIC_REPORTING = "generic_reporting";

    public static final String COUCHBASE_CLUSTER = "10.102.74.163";
    public static final String COUCHBASE_BUCKET_NAME = "aggregate_reporting"; //"tracking";
    public static final String COUCHBASE_BUCKET_PASSWORD = "eaprm$123"; //"eadpprms1";

    public static final String CONFIG = "conf";
    public static final String YAML_CONFIG_PATH = CONFIG + "/" + "storm-tripper.yaml";
    public static final String AGGREGATE_FIELD_DELIMITER = " ";
}
