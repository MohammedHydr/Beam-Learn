package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
public class MapElement {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> numbers =
                pipeline.apply(Create.of(10, 20, 30, 40, 50));
        System.out.println();
        PCollection<Integer> output = applyTransform(numbers);
        output.apply("PrintResult", MapElements
                .into(TypeDescriptors.strings())
                .via((Integer i) -> {
                    System.out.println(i);
                    return i.toString();
                }));
        pipeline.run();
    }
    static PCollection<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(
                MapElements.into(TypeDescriptors.integers()).via((Integer i) -> i * 5));
    }
}
