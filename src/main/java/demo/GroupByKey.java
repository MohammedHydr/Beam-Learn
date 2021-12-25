package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class GroupByKey {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> words =
                pipeline.apply(
                        Create.of("apple", "ball", "car", "bear", "cheetah", "ant")
                );

        PCollection<KV<String, Iterable<String>>> output = applyTransform(words);


        output.apply("PrintResult", MapElements
                .into(TypeDescriptors.strings())
                .via((KV<String, Iterable<String>> i) -> {
                    System.out.println(i.getKey() + " " + i.getValue());
                    return i.toString();
                }));

        pipeline.run();
    }

    static PCollection<KV<String, Iterable<String>>> applyTransform(PCollection<String> input) {
        return input
                //.apply(MapElements.into(kvs(strings(), strings())).via(word -> KV.of(word.substring(0, 1), word)))
                .apply(ParDo.of(new SubStringWord()))
                .apply(org.apache.beam.sdk.transforms.GroupByKey.create());
    }

    static class SubStringWord extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element String words, OutputReceiver<KV<String, String>> out) {
            out.output(KV.of(words.substring(0, 1), words));
        }

    }
}

