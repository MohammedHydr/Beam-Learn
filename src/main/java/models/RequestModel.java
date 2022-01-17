package models;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.util.Objects;

@DefaultCoder(AvroCoder.class)

public class RequestModel {

    private String domain;
    private String widKey;
    private String location;

    public RequestModel() {
    }

    public RequestModel(String domain, String location, String widKey) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestModel that = (RequestModel) o;
        return domain.equals(that.domain) && widKey.equals(that.widKey) && location.equals(that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(domain, widKey, location);
    }
}
