package edu.princeton.cs.shared;

import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.Response;
import com.google.gwt.json.client.JSONArray;
import com.google.gwt.json.client.JSONParser;
import com.google.gwt.json.client.JSONValue;
import com.google.gwt.user.client.Window;

import edu.princeton.cs.client.SMFMailDir;
import edu.princeton.cs.shared.SMFEJsonRpc.SMFEJsonRpcException;
import edu.princeton.cs.shared.SMFEMail.SMFEMailException;

public class SMFListMessagesAsync implements RequestCallback {

	private SMFMailDir mailDir;
	
	public SMFListMessagesAsync(SMFMailDir mailDir) {
		this.mailDir = mailDir;
	}

	@Override
	public void onResponseReceived(Request request, Response response) {
		SMFEMail[] mailList = null;
		/*if (response.getStatusCode() != Response.SC_OK) {
			Window.alert("Error processing request! ["+response.getStatusCode()+"].");
			return;
		}*/
		String jsonResp = response.getText();
				jsonResp = "{\"result\":[";
		jsonResp += "{\"id\":\"msg_id2\","
		          +"\"sender_addr\":\"foo@bar.com\","
		          +"\"receiver_addrs\":[\"nobody@acme.com\", \"mickeymouse@disney.com\"],"
		          +"\"cc_addrs\":[\"tux@bar.com\"],"
		          +"\"bcc_addrs\":[\"me@bar.com\"],"
		          +"\"subject\":\"This is the subject\","
		          +"\"timestamp\":1388793600,"
		          +"\"handle\":\"8011d599ed984edb9115dd71b68402be\","
		          +"\"is_read\":false,"
		          +"\"has_attachments\":true,"
		          +"\"attachment_names\":[\"79e54e60bcf2142a4d7c3131e2ebeef774be7dceb643f83ae2d16ee31e3e3dee\"]}";
		jsonResp += ",";
		jsonResp += "{\"id\":\"msg_id2\","
		          +"\"sender_addr\":\"foo@bar.com\","
		          +"\"receiver_addrs\":[\"nobody@acme.com\", \"mickeymouse@disney.com\"],"
		          +"\"subject\":\"This is the subject\","
		          +"\"timestamp\":1388793600,"
		          +"\"handle\":\"8011d599ed984edb9115dd71b68402be\","
		          +"\"is_read\":true,"
		          +"\"has_attachments\":false}";
		jsonResp += "]}";
		Window.alert(jsonResp);
		JSONValue jsonVal = null;
		try {
			jsonVal = SMFEJsonRpc.decodeRespose(jsonResp);
			jsonResp = jsonVal.toString();
		}
		catch (SMFEJsonRpcException e) {
			Window.alert(e.getMessage());
		}
		JSONValue listObj =JSONParser.parseLenient(jsonResp);
		if (listObj == null) {
			return;
		}
		JSONArray jsonMailList = null;
		if ((jsonMailList = listObj.isArray()) != null) {
			int mailListLen = jsonMailList.size();
			mailList = new SMFEMail[mailListLen];
			for (int i=0; i<mailListLen; i++) {
				String mailJson = jsonMailList.get(i).toString();
				try {
					mailList[i] = new SMFEMail(mailJson);
				} catch (SMFEMailException e) {
					Window.alert("Error Listing Mail Directory: "+e.getMessage());
					return;
				}
			}
		}
		else {
			return;
		}
		mailDir.loadDirCallback(mailList);
	}

	@Override
	public void onError(Request request, Throwable exception) {
		Window.alert(exception.getMessage());
	}

}
