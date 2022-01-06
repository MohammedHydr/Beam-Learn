package pipelines;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.maxmind.geoip2.model.CityResponse;
import models.RequestJson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.log4j.BasicConfigurator;
import pipelines.steps.ParseJsonRequests;

import java.util.ArrayList;
import java.util.List;

import static helpers.LocateIP.getLocation;

/**
 * An example that reads http requests from json file, and counts the number of the requests in each country.
 *
 *
 * <p>Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 * <p>To execute this pipeline locally, specify the BigQuery table for the output with the form:
 *
 * <pre>{@code
 * --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID // cognativex-intern:analysis.location
 * The input is a local file:
 * --input=requests_2021_12_22.json
 * }</pre>
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 */
public class LocationToBigQueryIOPipeline {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        RequestsPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(RequestsPipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.read().from(options.getInput()))
                .apply(new CountIPTransform())
                .apply(ParDo.of(new FormatCountIPsFn()))
                .apply(new LocationToBigQueryTransform());

        p.run().waitUntilFinish();
    }

    public interface RequestsPipelineOptions extends DataflowPipelineOptions {
        @Description("File path")
        @Validation.Required
        String getInput();

        void setInput(String value);

        @Description("Output")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

    /**
     * Format the results to a TableRow, to save to BigQuery.
     */
    static class FormatCountIPsFn extends DoFn<KV<String, Long>, TableRow> {
        @ProcessElement
        public void processElement(@Element KV<String, Long> locationCount, OutputReceiver<TableRow> table) {
            TableRow row =
                    new TableRow()
                            .set("country", locationCount.getKey())
                            .set("count", locationCount.getValue());
            table.output(row);
        }
    }

    /**
     * Reads rows from a http requests json file, and finds the country of the ip of
     * each request then count the countries.
     */
    public static class CountIPTransform
            extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> httpRequest) {
            PCollection<String> Country = httpRequest.apply(ParDo.of(new ParseJsonRequests()))
                    .apply(ParDo.of(new IpToLocationFn()));
            PCollection<KV<String, Long>> CountryCount = Country.apply(Count.perElement());
            return CountryCount;
        }
    }

    /**
     * Convert the ip to location by calling the getLocation Fn
     */
    static class IpToLocationFn extends DoFn<RequestJson, String> {
        @ProcessElement
        public void processElement(@Element RequestJson requestJson, OutputReceiver<String> location) {
            if (requestJson != null) {
                String ip = requestJson.getIp();
                CityResponse city = getLocation(ip);
                if (city != null && city.getCountry() != null)
                    location.output(city.getCountry().getName());
                else System.out.println("No country found/Ip not found " + ip);//ToDO use the beam counters
            } else System.out.println("No Object found");
        }
    }

    /**
     *  Build the table schema for the output table and transform the @TableRow in the  bigquery
     */
    static class LocationToBigQueryTransform
            extends PTransform<PCollection<TableRow>, PCollection<TableRow>>{
        @Override
        public PCollection<TableRow> expand(PCollection<TableRow> input) {

            RequestsPipelineOptions options = (RequestsPipelineOptions)input.getPipeline().getOptions();
            List<TableFieldSchema> fields = new ArrayList<>();
            fields.add(new TableFieldSchema().setName("country").setType("STRING"));
            fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
            TableSchema schema = new TableSchema().setFields(fields);
            input.apply(BigQueryIO.writeTableRows()
                    .to(options.getOutput())
                    .withSchema(schema)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
            return input;
        }
    }
}
