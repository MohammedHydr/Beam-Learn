package helpers;

import com.maxmind.geoip2.exception.GeoIp2Exception;
import junit.framework.TestCase;

import java.io.IOException;
import java.net.URISyntaxException;

import static helpers.LocateIP.getLocation;

public class LocateIPTest extends TestCase {

    public void testGetLocation() throws IOException, GeoIp2Exception, URISyntaxException {

        assertEquals(getLocation("107.77.212.18").getCountry().getName(),"United States");
    }
}