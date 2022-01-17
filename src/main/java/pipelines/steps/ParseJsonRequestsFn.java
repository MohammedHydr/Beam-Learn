package pipelines.steps;

import helpers.JsonToRequest;
import models.RequestJson;
import org.apache.beam.sdk.transforms.DoFn;

public class ParseJsonRequestsFn extends DoFn<String, RequestJson> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<RequestJson> request) {
        RequestJson rj = JsonToRequest.Parse(element);
        if (rj != null)
            request.output(rj);
    }
}
