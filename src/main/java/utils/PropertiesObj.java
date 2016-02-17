package utils;

import java.util.*;

/**
 * Created by yiguo on 2/8/16.
 */
public class PropertiesObj {
    private List<List<String>> aggregateFieldsList;
    private List<String> acceptedEventTypeList;
    private int maxBatchSize;
    private int maxSpoutPending;

    private Set<String> acceptedEventTypeSet = new HashSet<String>();

    public List<List<String>> getAggregateFieldsList() {
        return aggregateFieldsList;
    }

    public void setAggregateFieldsList(List<List<String>> aggregateFieldsList) {
        this.aggregateFieldsList = aggregateFieldsList;
        makeTidyAggregateFieldsList();
    }

    public List<String> getAcceptedEventTypeList() {
        return acceptedEventTypeList;
    }

    public void setAcceptedEventTypeList(List<String> acceptedEventTypeList) {
        this.acceptedEventTypeList = acceptedEventTypeList;
        for(String acceptedEventType : acceptedEventTypeList) {
            acceptedEventTypeSet.add(acceptedEventType);
        }
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

    public Set<String> getAcceptedEventTypeSet() {
        return acceptedEventTypeSet;
    }

    public void setAcceptedEventTypeSet(Set<String> acceptedEventTypeSet) {
        this.acceptedEventTypeSet = acceptedEventTypeSet;
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
