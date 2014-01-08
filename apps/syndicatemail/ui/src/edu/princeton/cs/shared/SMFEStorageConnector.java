package edu.princeton.cs.shared;

import com.google.gwt.http.client.RequestBuilder;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.http.client.URL;
import com.google.gwt.user.client.Window;

/**
 * This class maintains an HTTP connection to the backend server.
 * Provides an interface to initialize an HTTP connection and read/write
 * raw bytes to the connection.
 * @author wathsala
 *
 */
public class SMFEStorageConnector {
	
	private final String backend_url = "http://127.0.0.1/cgi-bin/test.sh";
	private String url;
	private static SMFEStorageConnector conn = null;
	private RequestBuilder builder = null;
	
	protected  SMFEStorageConnector() {
		url = URL.encode(backend_url);
	    builder = new RequestBuilder(RequestBuilder.POST, url);
	}
	
	public void write(String jsonStr, RequestCallback callback) throws RequestException {
		builder.sendRequest(jsonStr, callback);
	}
	
	public static SMFEStorageConnector getStorageConnector() {
		if (conn != null)
			return conn;
		conn = new SMFEStorageConnector();
		return conn;
	}
}
