package utils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by yiguo on 1/27/16.
 */
public class StreamPrinter extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        System.out.println(tuple.get(0));
    }
}
