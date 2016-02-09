package utils;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by yiguo on 2/8/16.
 */
public class PropertiesObj {
    private List<List<String>> aggregateFieldsList;
    private int maxBatchSize;
    private int maxSpoutPending;

    public List<List<String>> getAggregateFieldsList() {
        return aggregateFieldsList;
    }

    public void setAggregateFieldsList(List<List<String>> aggregateFieldsList) {
        this.aggregateFieldsList = aggregateFieldsList;
        makeTidyAggregateFieldsList();
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public int getMaxSpoutPending() {
        return maxSpoutPending;
    }

    public void setMaxSpoutPending(int maxSpoutPending) {
        this.maxSpoutPending = maxSpoutPending;
    }

    private void makeTidyAggregateFieldsList() {
        HashSet<String> aggregateFieldsSet = new HashSet<String>();
        List<List<String>> tidyAggregateFieldsList = new LinkedList<List<String>>();
        for(List<String> aggregateFields : aggregateFieldsList){
            Collections.sort(aggregateFields);
            StringBuilder sb = new StringBuilder();
            for(String field : aggregateFields){
                sb.append(field);
            }
            if(!aggregateFieldsSet.contains(sb.toString())){
                tidyAggregateFieldsList.add(aggregateFields);
                aggregateFieldsSet.add(sb.toString());
            }
        }
        this.aggregateFieldsList = tidyAggregateFieldsList;
    }
}
