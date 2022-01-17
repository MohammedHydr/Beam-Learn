package helpers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.maxmind.geoip2.model.CityResponse;
import models.RequestJson;

import java.util.Map;

import static helpers.UrlService.getQueryMap;

public class JsonToRequest {

    public static RequestJson Parse(String element) {
        if (element != null && element != "") {
            JsonObject jsonObject = new JsonParser().parse(element).getAsJsonObject();
            JsonElement httpRequest = jsonObject.get("httpRequest");
            if (httpRequest != null) {
                JsonElement elm = httpRequest.getAsJsonObject();
                JsonElement remoteIp = httpRequest.getAsJsonObject().get("remoteIp");
                JsonElement requestUrl = httpRequest.getAsJsonObject().get("requestUrl");
                if (requestUrl != null && requestUrl.getAsString() != "") {
                    Map<String, String> parms = getQueryMap(requestUrl.getAsString());
                    if (parms == null)
                        System.out.println("Error in parse url parms == null " + requestUrl.getAsString());
                    else if (parms.containsKey("apd") && parms.containsKey("wky")) {
                        String domain = parms.get("apd").toString();
                        String widgetKey = parms.get("wky").toString();
                        RequestJson rJson = new RequestJson(remoteIp.getAsString(), requestUrl.getAsString(), domain, widgetKey);
                        return rJson;
                    } else {
                        System.out.println("Error in parse url not contain apd or widgetKey " + requestUrl.getAsString());
                    }
                }
            }
        }
        return null;
    }
}
