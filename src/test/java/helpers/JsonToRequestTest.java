package helpers;

import junit.framework.TestCase;
import models.RequestJson;

import static helpers.JsonToRequest.Parse;


public class JsonToRequestTest extends TestCase {
    public void testParseString(){
        String link = "{\"httpRequest\":{\"cacheFillBytes\":\"520\",\"cacheLookup\":true,\"latency\":\"0.171373s\",\"referer\":\"https://m.kooora.com/\",\"remoteIp\":\"107.77.212.118\",\"requestMethod\":\"GET\",\"requestSize\":\"450\",\"requestUrl\":\"https://log.cognativex.com/pixel.png?kc=wi&cxv=10&apd=kooora.com&uid=f943723e-05f6-4f4f-8256-74223f5deba6&cxnid=undefined&ptd=120aee71-6e2f-40fb-b60c-a3d3be6832e8&cd=1640131200398&scr=375x812|375x812|32&cu=https%3A%2F%2Fm.kooora.com%2F%3Fm%3D2748901&ex=1096297,1096124,1096176&wid=5116448670220288&wky=Match-Details-Mobile&wvn=8&wsz=5\",\"responseSize\":\"520\",\"status\":200,\"userAgent\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1\"},\"insertId\":\"6dud28flj6yki\",\"jsonPayload\":{\"@type\":\"type.googleapis.com/google.cloud.loadbalancing.type.LoadBalancerLogEntry\",\"statusDetails\":\"response_sent_by_backend\"},\"logName\":\"projects/cognativex/logs/requests\",\"receiveTimestamp\":\"2021-12-22T00:00:00.953792809Z\",\"resource\":{\"labels\":{\"backend_service_name\":\"\",\"forwarding_rule_name\":\"cognativex-logs-balancer-forwarding-rule-3\",\"project_id\":\"cognativex\",\"target_proxy_name\":\"cognativex-logs-balancer-target-proxy-3\",\"url_map_name\":\"cognativex-logs-balancer\",\"zone\":\"global\"},\"type\":\"http_load_balancer\"},\"severity\":\"INFO\",\"spanId\":\"903fc7e24f93843b\",\"timestamp\":\"2021-12-22T00:00:00.612441Z\",\"trace\":\"projects/cognativex/traces/d7d21a887f81fe1af174ed158622f5a2\"}\n";
//        RequestJson testModelObject = new RequestJson("107.77.212.118","https://log.cognativex.com/pixel.png?kc=" +
//                "wi&cxv=10&apd=kooora.com&uid=f943723e-05f6-4f4f-8256-74223f5" +
//                "deba6&cxnid=undefined&ptd=120aee71-6e2f-40fb-b60c-a3d3be6832e8&" +
//                "cd=1640131200398&scr=375x812|375x812|32&cu=https%3A%2F%2Fm.kooora.c" +
//                "om%2F%3Fm%3D2748901&ex=1096297,1096124," +
//                "1096176&wid=5116448670220288&wky=Match-Details-Mobile&wvn=8&wsz=5"
//                ,"kooora.com","Match-Details-Mobile" );
//        assertEquals(Parse(link), testModelObject);// required to generate equals in the RequestJson class
        RequestJson rj =Parse(link);
        assertEquals(rj.getIp(), "107.77.212.118");
        assertEquals(rj.getUrl(), "https://log.cognativex.com/pixel.png?kc=" +
                "wi&cxv=10&apd=kooora.com&uid=f943723e-05f6-4f4f-8256-74223f5" +
                "deba6&cxnid=undefined&ptd=120aee71-6e2f-40fb-b60c-a3d3be6832e8&" +
                "cd=1640131200398&scr=375x812|375x812|32&cu=https%3A%2F%2Fm.kooora.c" +
                "om%2F%3Fm%3D2748901&ex=1096297,1096124," +
                "1096176&wid=5116448670220288&wky=Match-Details-Mobile&wvn=8&wsz=5");
        assertEquals(rj.getDomain(), "kooora.com");
        assertEquals(rj.getWidgetKey(), "Match-Details-Mobile");
    }
}