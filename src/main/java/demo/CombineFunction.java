package demo;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class CombineFunction {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> numbers = pipeline.apply(Create.of(10, 20, 50, 70, 90));

        PCollection<Double> output = applyTransform(numbers);
        output.apply("PrintResult", MapElements
                .into(TypeDescriptors.strings())
                .via((Double i) -> {
                    System.out.println(i);
                    return i.toString();
                }));
        pipeline.run();
    }
    static PCollection<Double> applyTransform(PCollection<Integer> input) {
        return input.apply(Combine.globally(new AverageFn()));
    }

    static class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {
        static class Accum implements Serializable {
            int sum = 0;
            int count = 0;

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Accum accum = (Accum) o;
                return sum == accum.sum &&
                        count == accum.count;
            }

            @Override
            public int hashCode() {
                return Objects.hash(sum, count);
            }
        }

        @Override
        public Accum createAccumulator() {
            return new Accum();
        }

        @Override
        public Accum addInput(Accum accumulator, Integer input) {
            accumulator.sum += input;
            accumulator.count++;

            return accumulator;
        }

        @Override
        public Accum mergeAccumulators(Iterable<Accum> accumulators) {
            Accum merged = createAccumulator();

            for (Accum accumulator : accumulators) {
                merged.sum += accumulator.sum;
                merged.count += accumulator.count;
            }

            return merged;
        }

        @Override
        public Double extractOutput(Accum accumulator) {
            return ((double) accumulator.sum) / accumulator.count;
        }

    }
}
