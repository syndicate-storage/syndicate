/*
 * UG Name Util for Syndicate
 */
package SyndicateHadoop.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class UGNameUtil {
    
    private static String getIPAddress() throws SocketException {
        Enumeration e = NetworkInterface.getNetworkInterfaces();
        while(e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration ee = n.getInetAddresses();
            while(ee.hasMoreElements()) {
                InetAddress i = (InetAddress) ee.nextElement();
                
                if(i instanceof Inet4Address) {
                    if(i.isLoopbackAddress())
                        continue;
                    if(i.isMulticastAddress())
                        continue;
                    if(i.isSiteLocalAddress())
                        continue;
                
                    return i.getHostAddress();
                }
            }
        }
        
        return null;
    }
    
    public static String getUGName(String prefix) {
        String address = null;
        
        try {
            address = getIPAddress();
        } catch (SocketException ex) {}
        
        if(address == null || address.isEmpty()) {
            return null;
        } else {
            if(prefix == null)
                return address.replaceAll("\\.", "_");
            else
                return prefix + address.replaceAll("\\.", "_");
        }
    }
}
