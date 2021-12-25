package demo;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;

public class CoGroupByKeys {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> fruits =
                pipeline.apply("Fruits",
                        Create.of("apple", "banana", "cherry")
                );

        PCollection<String> countries =
                pipeline.apply("Countries",
                        Create.of("australia", "brazil", "canada")
                );

        PCollection<String> output = applyTransform(fruits, countries);

        output.apply("PrintResult", MapElements
                .into(TypeDescriptors.strings())
                .via((String i) -> {
                    System.out.println(i);
                    return i.toString();
                }));

        pipeline.run();
    }
    static PCollection<String> applyTransform(
            PCollection<String> fruits, PCollection<String> countries) {

        PCollection<KV<String, String>> fruitsCollection = fruits.apply("Fruits to KV", ParDo.of(new SubStringWord()));
        PCollection<KV<String, String>> CountriesCollection = countries.apply("Countries to KV", ParDo.of(new SubStringWord()));

        TupleTag<String> fruitsTag = new TupleTag<>();
        TupleTag<String> countriesTag = new TupleTag<>();

        return KeyedPCollectionTuple.of(fruitsTag, fruitsCollection)
                .and(countriesTag, CountriesCollection).apply(CoGroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {

        @ProcessElement
        public void processElement(
                @Element KV<String, CoGbkResult> element, OutputReceiver<String> out) {

            String alphabet = element.getKey();
            CoGbkResult coGbkResult = element.getValue();

            String fruit = coGbkResult.getOnly(fruitsTag);
            String country = coGbkResult.getOnly(countriesTag);

            out.output(WordsAlphabet(alphabet, fruit, country));
        }

    }));
}
    static class SubStringWord extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element String words, OutputReceiver<KV<String, String>> out) {
            out.output(KV.of(words.substring(0, 1), words));
        }
    }
    static String WordsAlphabet(String alphabet, String fruit, String country){
        return "WordsAlphabet{" +
                "alphabet='" + alphabet + '\'' +
                ", fruit='" + fruit + '\'' +
                ", country='" + country + '\'' +
                '}';
    }
}
