package pipelines;

import com.maxmind.geoip2.model.CityResponse;
import models.RequestJson;
import models.RequestModel;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.log4j.BasicConfigurator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static helpers.JsonToRequest.Parse;
import static helpers.LocateIP.getLocation;

public class DomainLocationPipeline {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        RequestsOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(RequestsOptions.class);

        run(options);
    }

    static void run(RequestsOptions options) {
        Pipeline p = Pipeline.create(options);
        p
                .apply("ReadLines", TextIO.read().from(options.getInput()))
                .apply(new DomainWidgetLocation())
                .apply(ParDo.of(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));


        p.run().waitUntilFinish();
    }

    static PCollection<KV<String, Iterable<RequestModel>>> applyTransform(PCollection<RequestModel> input) {
        return input
                .apply(ParDo.of(new RequestModelKey()))
                .apply(org.apache.beam.sdk.transforms.GroupByKey.create());
    }

    static class ParseFn extends DoFn<String, RequestModel> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<RequestModel> domainLocation) {
            RequestJson jsonModel = Parse(element);
            if (jsonModel != null) {
                String domain = jsonModel.getDomain();
                String wky = jsonModel.getWidgetKey();
                String ip = jsonModel.getIp();
                CityResponse response = getLocation(ip);
                if (response != null) {
                    String country = response.getCountry().getName();
                    if (country == null) {
                        country = "Unkown";
                    }
                    RequestModel rModel = new RequestModel(domain, country, wky);
                    domainLocation.output(rModel);
                }
                System.out.println("No Response");
            }
        }
    }

    static class RequestModelKey extends DoFn<RequestModel, KV<String, RequestModel>> {
        @ProcessElement
        public void processElement(@Element RequestModel rModel, OutputReceiver<KV<String, RequestModel>> data) {
            data.output(KV.of(rModel.getDomain(), rModel));
        }
    }

    public static class DomainWidgetLocation
            extends PTransform<PCollection<String>, PCollection<KV<String, Iterable<RequestModel>>>> {
        @Override
        public PCollection<KV<String, Iterable<RequestModel>>> expand(PCollection<String> httpRequest) {
            PCollection<RequestModel> data = httpRequest.apply(ParDo.of(new ParseFn()));
            PCollection<KV<String, Iterable<RequestModel>>> urls = applyTransform(data);
            return urls;
        }
    }

    static class FormatAsTextFn extends DoFn<KV<String, Iterable<RequestModel>>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<RequestModel>> element, OutputReceiver<String> receiver) {
            List<RequestModel> result = new ArrayList<RequestModel>();
            Iterable<RequestModel> elementModel = element.getValue();
            if (elementModel != null) {
                for (RequestModel rModel : elementModel) {
                    result.add(rModel);
                }
                Map<String, Integer> mapOfWidgetKeys = new HashMap<String, Integer>();
                Map<String, Integer> mapOfLocations = new HashMap<String, Integer>();
                for (RequestModel key : result) {
                    if (key != null) {
                        if (key.getWidKey() != null) {
                            if (!mapOfWidgetKeys.containsKey(key.getWidKey())) {
                                mapOfWidgetKeys.put(key.getWidKey(), 1);
                            } else {
                                mapOfWidgetKeys.put(key.getWidKey(), mapOfWidgetKeys.get(key.getWidKey()) + 1);
                            }
                        }
                        if (key.getLocation() != null) {
                            if (!mapOfLocations.containsKey(key.getLocation())) {
                                mapOfLocations.put(key.getLocation(), 1);
                            } else {
                                if (mapOfLocations.get(key.getLocation()) != null)
                                    mapOfLocations.put(key.getLocation(), mapOfLocations.get(key.getLocation()) + 1);
                            }
                        }

                    }
                }
                receiver.output(element.getKey() + ":" + mapOfLocations + "," + mapOfWidgetKeys);
            }
        }
    }
}

