package model;

import java.util.List;

/**
 * Created by yiguo on 12/15/15.
 */
public class TripperPropObj {
    private List<String> aggregateFieldsList;

    public TripperPropObj(){

    }

    public List<String> getAggregateFieldsList() {
        return aggregateFieldsList;
    }

    public void setAggregateFieldsList(List<String> aggregateFieldsList) {
        this.aggregateFieldsList = aggregateFieldsList;
    }
}
