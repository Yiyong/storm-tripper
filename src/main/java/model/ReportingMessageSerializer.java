package model;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectWriter;
import storm.trident.state.Serializer;

/**
 * Created by yiguo on 1/19/16.
 */
public class ReportingMessageSerializer implements Serializer<ReportingMessage> {
    private static final long serialVersionUID = -1864797806751163034L;
    private static ReportingMessageSerializer reportingMessageSerializer;
    private static ObjectMapper mapper = new ObjectMapper();

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
        JsonNode objNode = mapper.createObjectNode();
        ObjectWriter objectWriter = mapper.writer();
        byte[] bytes = null;
        try {
            bytes = objectWriter.writeValueAsBytes(objNode);
        } catch (JsonProcessingException e) {
            System.err.println("Error in serializing raw events: " + e);
        }
        return bytes;
    }

    @Override
    public ReportingMessage deserialize(byte[] b) {
        try
        {
            return mapper.readValue(b, ReportingMessage.class);
        } catch (Exception e)
        {
            System.err.println("Invalid reporting events: " + e);
        }
        return null;
    }
}
