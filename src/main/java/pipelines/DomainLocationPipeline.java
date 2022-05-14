package pipelines;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import models.RequestModel;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import java.util.*;

import static helpers.LocateIP.getLocation;
import static helpers.UrlService.getQueryMap;


public class DomainLocationPipeline {
    public static void main(String[] args) {
        RequestsOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(RequestsOptions.class);

        run(options);
    }
    static void run(RequestsOptions options) {
        // Create a PipelineOptions object that reads TextIO and write a TextIO and apply the transform DoFn and widgets class
        Pipeline p = Pipeline.create(options);
//        p
//                .apply("ReadLines", TextIO.read().from(options.getInput()))
//                .
//                .apply("WriteCounts", TextIO.write().to(options.getOutput()));


        p.run().waitUntilFinish();
    }

    public static String getUrl (String element){
        if (element != null && element != "") {
            JsonObject jsonObject = new JsonParser().parse(element).getAsJsonObject();
            JsonElement httpRequest = jsonObject.get("httpRequest");
            if (httpRequest != null) {
                JsonElement requestUrl = httpRequest.getAsJsonObject().get("requestUrl");
                if (requestUrl != null)
                    return requestUrl.getAsString();
            }
        }
        return "No Url found";
    }

    public static String getIP (String element){
        if (element != null && element != "") {
            JsonObject jsonObject = new JsonParser().parse(element).getAsJsonObject();
            JsonElement httpRequest = jsonObject.get("httpRequest");
            if (httpRequest != null) {
                JsonElement remoteIp = httpRequest.getAsJsonObject().get("remoteIp");
                if (remoteIp != null)
                    return remoteIp.getAsString();
            }
        }
        return "No IP found";
    }

    static class ParseEventFn extends DoFn<String, RequestModel> {
        public void processElement(@Element String element, OutputReceiver<RequestModel> domainLocation) {
            String url = getUrl(element);
            if (url != null && url != "") {
                Map<String, String> parms = getQueryMap(url);
                if (parms == null)
                    System.out.println("Error in parse url parms == null " + url);
                else if (parms.containsKey("apd") && parms.containsKey("wky")) {
                    String domain = parms.get("apd");
                    String wky = parms.get("wky");
                    String ip = getIP(element);
                    String country = getLocation(ip).getCountry().getName();
                    domainLocation.output(new RequestModel(domain, wky, country));

                } else {
                    System.out.println("Error in parse url not contain apd " + url);
                }
            }
        }
    }
    static class RequestModelKey extends DoFn<RequestModel, KV<String, RequestModel>>{
        public void processElement(@Element RequestModel rModel, OutputReceiver <KV<String,RequestModel>> data) {
            data.output(KV.of(rModel.getDomain(),rModel));
        }
    }

    public static class DomainWidgetLocation
            extends PTransform<PCollection<String>, PCollection<KV<String, Iterable<RequestModel>>>> {
        @Override
        public PCollection<KV<String, Iterable<RequestModel>>> expand(PCollection<String> httpRequest) {
            PCollection<RequestModel> data = httpRequest.apply(ParDo.of(new ParseEventFn()));
            PCollection<KV<String, Iterable<RequestModel>>> urls = applyTransform(data);
            return urls;
        }
    }
    static PCollection<KV<String, Iterable<RequestModel>>> applyTransform(PCollection<RequestModel> input) {
        return input
                .apply(ParDo.of(new RequestModelKey()))
                .apply(org.apache.beam.sdk.transforms.GroupByKey.create());
    }

    static class FormatAsTextFn extends DoFn<KV<String, Iterable<RequestModel>>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<RequestModel>> element, OutputReceiver<String> receiver) {
            List<RequestModel> result = new ArrayList<RequestModel>();
            for (RequestModel rModel : element.getValue()) {
                result.add(rModel);
            }
        }
    }
}

