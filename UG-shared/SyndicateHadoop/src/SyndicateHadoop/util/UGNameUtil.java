/*
 * UG Name Util for Syndicate
 */
package SyndicateHadoop.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 *
 * @author iychoi
 */
public class UGNameUtil {
    
    private static String ipAddress = null;
    private static boolean ipAddressChecked = false;
    
    private static String getIPAddress() {
        if(!ipAddressChecked) {
            try {
                Enumeration e = NetworkInterface.getNetworkInterfaces();
                while (e.hasMoreElements()) {
                    NetworkInterface n = (NetworkInterface) e.nextElement();
                    Enumeration ee = n.getInetAddresses();
                    while (ee.hasMoreElements()) {
                        InetAddress i = (InetAddress) ee.nextElement();

                        if (i instanceof Inet4Address) {
                            if (i.isLoopbackAddress()) {
                                continue;
                            }
                            if (i.isMulticastAddress()) {
                                continue;
                            }
                            if (i.isSiteLocalAddress()) {
                                continue;
                            }

                            // get first ipaddress
                            ipAddress = i.getHostAddress();
                            ipAddressChecked = true;
                            return ipAddress;
                        }
                    }
                }
            } catch (SocketException ex) {
                ipAddress = null;
                ipAddressChecked = true;
                return null;
            }
        }
        
        return ipAddress;
    }
    
    public static String getUGName(String prefix) {
        String address = null;
        address = getIPAddress();
        
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
