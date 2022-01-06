package helpers;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;

public class LocateIP {
    public static DatabaseReader dbReader = null;

    static {
        URL resource = LocateIP.class.getClassLoader().getResource("GeoLite2-City.mmdb");

        File database = null;
        if (resource != null) {
            database = new File(resource.getFile());
        }

        try {
            dbReader = new DatabaseReader.Builder(database).build();
        } catch (IOException e) {
            e.printStackTrace();
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
