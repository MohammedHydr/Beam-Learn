package pipelines.steps;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import models.RequestJson;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Map;

import static helpers.UrlService.getQueryMap;

public class ParseJsonRequests extends DoFn<String, RequestJson> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<RequestJson> request) {

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
                        request.output(rJson);
                    } else {
                        System.out.println("Error in parse url not contain apd " + requestUrl.getAsString());
                    }
                }
            }
        }
    }
}
