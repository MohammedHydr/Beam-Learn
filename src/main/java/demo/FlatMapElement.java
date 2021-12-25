package demo;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
public class FlatMapElement {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> sentences =
                pipeline.apply(Create.of("Apache Beam", "Unified Batch and Streaming"));

        PCollection<String> output = applyTransform(sentences);
            output.apply("PrintResult", MapElements
                    .into(TypeDescriptors.strings())
                    .via((String i) -> {
                        System.out.println(i);
                        return i.toString();
                    }));
        pipeline.run();

    }
    static PCollection<String> applyTransform(PCollection<String> input) {
        return input.apply(
                FlatMapElements.into(TypeDescriptors.strings())
                        .via((String sentence) -> Arrays.asList(sentence.split(" ")))
        );
    }
}
