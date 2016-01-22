package model;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.storm.shade.org.apache.commons.lang.SerializationUtils;
import storm.trident.state.Serializer;

/**
 * Created by yiguo on 1/19/16.
 */
public class ReportingMessageSerializer implements Serializer<ReportingMessage> {
    private static final long serialVersionUID = -1864797806751163034L;
    private static ReportingMessageSerializer reportingMessageSerializer;

    private ReportingMessageSerializer(){

    }

    public static ReportingMessageSerializer getInstance(){
        if(reportingMessageSerializer == null){
            reportingMessageSerializer = new ReportingMessageSerializer();
        }
        return reportingMessageSerializer;
    }


    @Override
    public byte[] serialize(ReportingMessage obj) {
        byte[] bytes = SerializationUtils.serialize(obj);
        return bytes;
    }

    @Override
    public ReportingMessage deserialize(byte[] b) {
        return (ReportingMessage) SerializationUtils.deserialize(b);
    }
}
