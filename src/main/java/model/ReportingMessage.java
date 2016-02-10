package model;

import org.apache.commons.lang3.EnumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Yiyong on 12/6/15.
 */
public class ReportingMessage implements Serializable {

    private String messageId;
    private String destinationId;
    private String destinationIdentifier;
    private String segmentId;
    private String contentId;
    private String country;
    private String city;
    private long timestamp;

    private ReportingEventType type;
    private int count;

    public ReportingMessage(String messageId, String destinationId, String destinationIdentifier, String segmentId, String contentId, String country, String city) {
        this.messageId = messageId;
        this.destinationId = destinationId;
        this.destinationIdentifier = destinationIdentifier;
        this.segmentId = segmentId;
        this.contentId = contentId;
        this.country = country;
        this.city = city;
    }

    private Logger logger = LoggerFactory.getLogger(getClass());

    public String getContentId() {
        return contentId;
    }

    public void setContentId(String contentId) {
        this.contentId = contentId;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(String destinationId) {
        this.destinationId = destinationId;
    }

    public String getDestinationIdentifier() {
        return destinationIdentifier;
    }

    public void setDestinationIdentifier(String destinationIdentifier) {
        this.destinationIdentifier = destinationIdentifier;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(String segmentId) {
        this.segmentId = segmentId;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public ReportingEventType getType() {
        return type;
    }

    public void setType(String type) {
        if (EnumUtils.isValidEnum(ReportingEventType.class, type)) {
            this.type = ReportingEventType.valueOf(type);
        } else {
            this.type = ReportingEventType.UNKNOWN;
            logger.error("Unknown reporting event caught: " + type);
        }
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public String getAggregationKey(List<String> aggregateFields) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String field : aggregateFields) {
            if (field.equals("message")) {
                stringBuilder.append("msg").append(messageId).append(Constants.DELIMITER);
            }
            if (field.equals("destination")) {
                stringBuilder.append("dst").append(destinationId).append(Constants.DELIMITER);
            }
            if (field.equals("destination_identifier")) {
                stringBuilder.append("dsti").append(destinationIdentifier).append(Constants.DELIMITER);
            }
            if (field.equals("segment")) {
                stringBuilder.append("seg").append(segmentId).append(Constants.DELIMITER);
            }
            if (field.equals("content")) {
                stringBuilder.append("cnt").append(contentId).append(Constants.DELIMITER);
            }
            if (field.equals("country")) {
                stringBuilder.append("ctr").append(country).append(Constants.DELIMITER);
            }
            if (field.equals("city")) {
                stringBuilder.append("cty").append(city).append(Constants.DELIMITER);
            }
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }
}
