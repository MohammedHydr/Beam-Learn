package piplelines;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.log4j.BasicConfigurator;

import java.util.*;

import static helpers.UrlService.getQueryMap;

public class RequestsAnalysisPipeline {

    public static void main(String[] args) {
        BasicConfigurator.configure();
        Requests options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(Requests.class);
//        run(options);
//        options.setProject("cognativex-intern");
//        options.setRegion("us-central1");
//        options.setStagingLocation("gs://cx_intern/binaries/");
//        options.setGcpTempLocation("gs://cx_intern/temp/");
//        options.setRunner(DataflowRunner.class);

        // Create a PipelineOptions object that reads TextIO and write a TextIO and apply the transform DoFn and widgets class
        Pipeline p = Pipeline.create(options);
        p
                .apply("ReadLines", TextIO.read().from(options.getInput()))
                .apply(new Widgets())
                .apply(ParDo.of(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));


        p.run();
    }

    static PCollection<KV<String, Iterable<String>>> applyTransform(PCollection<String> input) {
        return input
                .apply(ParDo.of(new ParseUrl()))
                .apply(org.apache.beam.sdk.transforms.GroupByKey.create());
    }

    //output and input add in run configurations
    public interface Requests extends DataflowPipelineOptions {
        @Description("File path")
        @Validation.Required
        String getInput();

        void setInput(String value);

        @Description("Output")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

    // use arrayList to store the Iterable then use map to store the keys and count them
    static class FormatAsTextFn extends DoFn<KV<String, Iterable<String>>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<String>> element, OutputReceiver<String> receiver) {


            List<String> result = new ArrayList<String>();
            for (String str : element.getValue()) {
                result.add(str);
            }
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (String key : result) {
                if (!map.containsKey(key)) {
                    map.put(key, 1);
                } else {
                    map.put(key, map.get(key) + 1);
                }
            }
            receiver.output(element.getKey() + ":" + map);
        }
    }

    // call the ParseToJson and ParseUrl to transform the json string using class widget
    public static class Widgets
            extends PTransform<PCollection<String>, PCollection<KV<String, Iterable<String>>>> {
        @Override
        public PCollection<KV<String, Iterable<String>>> expand(PCollection<String> lines) {
            PCollection<String> data = lines.apply(ParDo.of(new ParseToJson()));
            PCollection<KV<String, Iterable<String>>> urls = applyTransform(data);
            return urls;
        }
    }

    // get the url from the http from json file
    static class ParseToJson extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> property) {

            if (element != null && element != "") {
                JsonObject jsonObject = new JsonParser().parse(element).getAsJsonObject();
                JsonElement httpRequest = jsonObject.get("httpRequest");
                if (httpRequest != null) {
                    JsonElement requestUrl = httpRequest.getAsJsonObject().get("requestUrl");
                    if (requestUrl != null)
                        property.output(requestUrl.getAsString());
                }
            }
        }
    }

    // parse the url using getQueryMap method to separate the url and store them in Map<String,String>
// then output a KV(ref of the url, widget key wky)
    static class ParseUrl extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element String url, OutputReceiver<KV<String, String>> domain) {
            if (url != null && url != "") {
                Map<String, String> parms = getQueryMap(url);
                if (parms == null)
                    System.out.println("Error in parse url parms == null " + url);
                else if (parms.containsKey("apd") && parms.containsKey("wky")) {
                    String ref = parms.get("apd").toString();
                    String wky = parms.get("wky").toString();
                    domain.output(KV.of(ref, wky));
                } else {
                    System.out.println("Error in parse url not contain apd " + url);
                }
            }
        }
    }
}
