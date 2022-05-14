package models;

public class RequestModel {
    private String domain;
    private String widKey;
    private String location;

    public RequestModel() {
    }

    public RequestModel(String domain, String widKey, String location) {
        this.domain = domain;
        this.widKey = widKey;
        this.location = location;
    }

    public String getDomain() {
        return domain;
    }

    public String getWidKey() {
        return widKey;
    }

    public String getLocation() {
        return location;
    }
}
