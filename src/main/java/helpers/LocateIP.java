package helpers;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;

public class LocateIP {

    private static final String DB_URL = "https://storage.googleapis.com/cx_intern/GeoLite2-City.mmdb";
    public static DatabaseReader dbReader = null;

    static {
        File database = null;
        if (System.getenv().containsKey("maxlocal")) {
            URL resource = LocateIP.class.getClassLoader().getResource("GeoLite2-City.mmdb");
            if (resource != null) {
                database = new File(resource.getFile());
                try {
                    dbReader = new DatabaseReader.Builder(database).build();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {

            try {
                InputStream stream = new URL(DB_URL).openStream();
                dbReader = new DatabaseReader.Builder(stream).withCache(new CHMCache()).build();
                URL resource = new URL(DB_URL);
                database = new File(resource.getFile());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static CityResponse getLocation(String ip) {
        CityResponse response = null;
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            if (dbReader != null) {
                response = dbReader.city(ipAddress);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        String countryName = response.getCountry().getName();
//        String cityName = response.getCity().getName();
//        String postal = response.getPostal().getCode();
//        String state = response.getLeastSpecificSubdivision().getName();

        return response;
    }
}
