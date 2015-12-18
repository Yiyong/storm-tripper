package model;

import java.util.List;

/**
 * Created by Yiyong on 12/6/15.
 */
public class ReportingMessage {
    private String messageId;
    private String contentId;
    private String country;

    private int impressions;
    private int clicks;

    public ReportingMessage(String messageId, String contentId, String country) {
        this.messageId = messageId;
        this.contentId = contentId;
        this.country = country;
    }

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

    public int getImpressions() {
        return impressions;
    }

    public void setImpressions(int impressions) {
        this.impressions = impressions;
    }

    public int getClicks() {
        return clicks;
    }

    public void setClicks(int clicks) {
        this.clicks = clicks;
    }

    public String getAggregationKey(List<String> aggregateFields) {
        StringBuilder stringBuilder = new StringBuilder();
        for(String field : aggregateFields){
            if (field.equals("message")) {
                stringBuilder.append("msg").append(messageId);
            }
            if (field.equals("content")) {
                stringBuilder.append("cnt").append(contentId);
            }
            if(field.equals("country")) {
                stringBuilder.append("ctr").append(country);
            }
        }
        return stringBuilder.toString();
    }
}
