package pipelines;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface RequestsOptions extends PipelineOptions {
    @Description("File path")
    @Validation.Required
    String getInput();

    void setInput(String value);

    @Description("Output")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
}
