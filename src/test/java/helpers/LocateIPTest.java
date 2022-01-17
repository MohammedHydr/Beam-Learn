package helpers;

import junit.framework.TestCase;

import static helpers.LocateIP.getLocation;

public class LocateIPTest extends TestCase {

    public void testGetLocation(){
        assertEquals(getLocation("107.77.212.18").getCountry().getName(),"United States");
    }
}