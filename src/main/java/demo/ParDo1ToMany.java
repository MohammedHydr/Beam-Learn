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

public class ParDo1ToMany {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> sentences =
                pipeline.apply(Create.of("Hello Beam", "It is awesome"));

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
        return input.apply(ParDo.of(new DoFn<String, String>(){
            @ProcessElement
            public void processElement(@Element String sentences,
                    OutputReceiver<String> newSentences) {
                for (String word : sentences.split(" ")) {
                    newSentences.output(word);
                }
            }
        }));
    }
}
