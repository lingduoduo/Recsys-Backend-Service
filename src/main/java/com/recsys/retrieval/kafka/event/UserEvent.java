package com.demo.retrieval.kafka.event;

public record UserEvent(
        String userId,
        int age,
        String gender,
        String occupation,
        String zipCode,
        long timestamp) implements MovieLensEvent {

    public String getUserId()     { return userId; }
    public int getAge()           { return age; }
    public String getGender()     { return gender; }
    public String getOccupation() { return occupation; }
    public String getZipCode()    { return zipCode; }
    public long getTimestamp()    { return timestamp; }
}
