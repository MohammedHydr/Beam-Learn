package demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class CombineSimpleFn {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> numbers = pipeline.apply(Create.of(10, 30, 50, 70, 90));

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
        return input.apply(Combine.globally(new SumIntegerFn()));
    }
    static class SumIntegerFn implements SerializableFunction<Iterable<Integer>, Integer> {
        @Override
        public Integer apply(Iterable<Integer> input) {
            int sum = 0;
            for (int number : input) {
                sum = sum + number;
            }
            return sum;
        }
    }
}
