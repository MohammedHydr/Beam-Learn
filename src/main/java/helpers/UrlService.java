package helpers;

import java.util.HashMap;
import java.util.Map;

public class UrlService {
    // function to split the url and put it in a map separated parts
    public static Map<String, String> getQueryMap(String query) {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params) {
            String[] ar = param.split("=");
            if (ar.length == 2) {
                String name = ar[0];
                String value = ar[1];
                map.put(name, value);
            } else
                System.out.println("Error in getquerymap " + query);
        }
        return map;
    }
}
