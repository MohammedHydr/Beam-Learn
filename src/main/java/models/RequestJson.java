package models;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.util.Objects;

@DefaultCoder(AvroCoder.class)

public class RequestJson {

    public @Nullable String ip;
    public @Nullable String url;
    public @Nullable String domain;
    public @Nullable String widgetKey;

    public RequestJson() {
    }

    public RequestJson(String ip, String url, String domain, String widgetKey) {
        this.ip = ip;
        this.url = url;
        this.domain = domain;
        this.widgetKey = widgetKey;
    }

    public String getIp() {
        return ip;
    }

    public String getUrl() {
        return url;
    }

    public String getDomain() {
        return domain;
    }

    public String getWidgetKey() {
        return widgetKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestJson that = (RequestJson) o;
        return Objects.equals(ip, that.ip) && Objects.equals(url, that.url) && Objects.equals(domain, that.domain) && Objects.equals(widgetKey, that.widgetKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, url, domain, widgetKey);
    }
}
