package utils;

/**
 * Created by yiguo on 1/19/16.
 */
public class Utils {
    public static long getCurrentTimeStamp(){
        long unixTime = System.currentTimeMillis() / 1000L;
        return unixTime;
    }
}
