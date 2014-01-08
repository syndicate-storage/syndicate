package edu.princeton.cs.shared;

import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.Response;
import com.google.gwt.json.client.JSONValue;
import com.google.gwt.user.client.Window;

import edu.princeton.cs.client.SMFMailDir;
import edu.princeton.cs.shared.SMFEJsonRpc.SMFEJsonRpcException;

public class SMFDeleteMessageAsync implements RequestCallback {

	private SMFMailDir dir;
	
	public SMFDeleteMessageAsync(SMFMailDir dir) {
		this.dir = dir;
	}
	
	@Override
	public void onResponseReceived(Request request, Response response) {
		/*if (response.getStatusCode() != Response.SC_OK) {
			Window.alert("Error processing request! ["+response.getStatusCode()+"].");
			return;
		}*/
		//String jsonStr = response.getText();
		String jsonStr = "{\"result\": true}";
		try {
			JSONValue val = SMFEJsonRpc.decodeRespose(jsonStr);
			if (SMFEJsonRpc.getBooleanValue(val)) {
				dir.reloadDirFromStorage();
			}
		} 
		catch (SMFEJsonRpcException e) {
			Window.alert(e.getMessage());
		}
	}

	@Override
	public void onError(Request request, Throwable exception) {
		
	}

}
