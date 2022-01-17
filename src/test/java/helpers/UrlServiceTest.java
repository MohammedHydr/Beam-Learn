package helpers;

import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

import static helpers.UrlService.getQueryMap;

public class UrlServiceTest extends TestCase {

    public void testGetQueryMap() {
        String url = "https://log.cognativex.com/pixel.png?kc=" +
                "wi&cxv=10&apd=kooora.com&uid=f943723e-05f6-4f4f-8256-74223f5" +
                "deba6&cxnid=undefined&ptd=120aee71-6e2f-40fb-b60c-a3d3be6832e8&" +
                "cd=1640131200398&scr=375x812|375x812|32&cu=https%3A%2F%2Fm.kooora.c" +
                "om%2F%3Fm%3D2748901&ex=1096297,1096124," +
                "1096176&wid=5116448670220288&wky=Match-Details-Mobile&wvn=8&wsz=5";
        Map<String, String> map = getQueryMap(url);
        String apd = map.get("apd").toString();
        String wky = map.get("wky").toString();
        assertEquals(apd, "kooora.com");
        assertEquals(wky, "Match-Details-Mobile");
    }
}