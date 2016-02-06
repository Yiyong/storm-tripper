package utils;

import model.ReportingEventType;

/**
 * Created by yiguo on 1/19/16.
 */
public class Utils {
    public static long getCurrentTimeStamp(){
        long unixTime = System.currentTimeMillis() / 1000L;
        return unixTime;
    }

    public static String reportTypeToCouchbaseKey (String type){
        return type.toLowerCase() + "s";
    }

    public static String reportTypeToCouchbaseKey (ReportingEventType type){
        return type.name().toLowerCase() + "s";
    }

}
