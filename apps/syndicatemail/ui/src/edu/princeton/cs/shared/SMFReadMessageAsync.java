package edu.princeton.cs.shared;

import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.Response;
import com.google.gwt.user.client.Window;

import edu.princeton.cs.client.SMFMailDir;
import edu.princeton.cs.client.SMFMailDisplay;
import edu.princeton.cs.shared.SMFEJsonRpc.SMFEJsonRpcException;
import edu.princeton.cs.shared.SMFEMail.SMFEMailException;

public class SMFReadMessageAsync implements RequestCallback  {

	SMFMailDir dir;
	
	public SMFReadMessageAsync(SMFMailDir dir) {
		this.dir = dir;
	}
	
	@Override
	public void onResponseReceived(Request request, Response response) {
		/*if (response.getStatusCode() != Response.SC_OK) {
			Window.alert("Error processing request! ["+response.getStatusCode()+"].");
			return;
		}*/
		//String jsonStr = response.getText();
		String jsonStr = "{\"result\":{\"id\":\"msg_id2\","
		          +"\"sender_addr\":\"foo@bar.com\","
		          +"\"receiver_addrs\":[\"nobody@acme.com\", \"mickeymouse@disney.com\"],"
		          +"\"cc_addrs\":[\"tux@bar.com\"],"
		          +"\"bcc_addrs\":[\"me@bar.com\"],"
		          +"\"subject\":\"This is the subject\","
		          +"\"body\":\"This is the body\","
		          +"\"timestamp\":1388897434,"
		          +"\"handle\":\"8011d599ed984edb9115dd71b68402be\","
		          +"\"attachment_names\":[\"79e54e60bcf2142a4d7c3131e2ebeef774be7dceb643f83ae2d16ee31e3e3dee\"]}}";
		try {
			Window.alert(">>>>>> "+jsonStr);
			jsonStr = SMFEJsonRpc.decodeRespose(jsonStr).toString();
			SMFEMail mail = new SMFEMail(jsonStr);
			dir.loadMailDisplayCallback(mail);
		} catch (SMFEMailException e) {
			Window.alert(e.getMessage());
		} catch (SMFEJsonRpcException e) {
			Window.alert(e.getMessage());
		}
	}

	@Override
	public void onError(Request request, Throwable exception) {
		
	}

}
