package pipelines;

import com.maxmind.geoip2.model.CityResponse;
import models.RequestJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import pipelines.steps.ParseJsonRequestsFn;
import static helpers.LocateIP.getLocation;

public class LocationAnalysis {
    public static void main(String[] args) {
        RequestsOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(RequestsOptions.class);

        run(options);
    }

    static void run(RequestsOptions options) {
        // Create a PipelineOptions object that reads TextIO and write a TextIO and apply the transform DoFn and widgets class
        Pipeline p = Pipeline.create(options);
        p
                .apply("ReadLines", TextIO.read().from(options.getInput()))
                .apply(new CountIPs())
                .apply(ParDo.of(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    public static class CountIPs
            extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> httpRequest) {
            PCollection<String> Country = httpRequest.apply(ParDo.of(new ParseJsonRequestsFn())).apply(ParDo.of(new Countries()));
            PCollection<KV<String, Long>> CountryCount = Country.apply(Count.perElement());
            return CountryCount;
        }
    }

    static class FormatAsTextFn extends DoFn<KV<String, Long>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, Long> element, OutputReceiver<String> receiver) {
            receiver.output(element.getKey() + ": " + element.getValue());
        }
    }

    static class Countries extends DoFn<RequestJson, String> {
        @ProcessElement
        public void processElement(@Element RequestJson requestJson, OutputReceiver<String> location){
            if (requestJson != null) {
                String ip = requestJson.getIp();
                CityResponse city = getLocation(ip);
                if (city != null && city.getCountry() != null)
                    location.output(city.getCountry().getName());
                else System.out.println("No country found/Ip not found " + ip);
            } else System.out.println("No Object found");
        }
    }
}
