package edu.princeton.cs.shared;

import com.google.gwt.json.client.JSONArray;
import com.google.gwt.json.client.JSONBoolean;
import com.google.gwt.json.client.JSONNumber;
import com.google.gwt.json.client.JSONObject;
import com.google.gwt.json.client.JSONParser;
import com.google.gwt.json.client.JSONString;
import com.google.gwt.json.client.JSONValue;
import com.google.gwt.user.client.Window;

/**
 * This is simple class to represent a single Email.
 * @author wathsala
 *
 */

public class SMFEMail {
	
	public class SMFEMailException extends Exception{

		private static final long serialVersionUID = 1L;
		private String msg;
		
		public SMFEMailException(String msg) {
			this.msg = msg;
		}
		
		public String getMessage() {
			return this.msg;
		}
		
		public String toString() {
			return this.msg;
		}
	}

	public String[] getAttachements() {
		return attachements;
	}

	public String getBody() {
		return body;
	}

	private String msg_id;
	private String msgHandle;
	private String sndrAddr;
	private String[] rcptAddrs;
	private String[] ccAddrs;
	private String[] bccAddrs;
	private String[] attachements;
	private String body;
	private String subject;
	private boolean hasAttachements;
	private boolean isRead;
	private long msgts;
	
	public SMFEMail(String msgHandle, String sndrAddr, String[] rcptAddrs, String body, String attachments[], long msgts) {
		this.msgHandle = msgHandle;
		this.sndrAddr = sndrAddr;
		this.rcptAddrs = rcptAddrs;
		this.msgts = msgts;
	}
	
	public SMFEMail(String jsonStr) throws SMFEMailException{
		if (jsonStr == null)
			throw new SMFEMailException("Invalid JSON String.");
		Window.alert(">>>>> "+jsonStr);
		
		JSONArray rcptArray = null, ccArray = null, bccArray = null, attachArray = null;
		JSONString json_sender = null, json_handle = null, json_subject = null, json_body = null, 
				json_msgid = null;
		JSONNumber json_ts = null;
		JSONBoolean json_read = null, json_has_attachements = null;
		JSONObject mailObj = null;
		JSONValue tmpVal = null;
		
		JSONValue mailVal = JSONParser.parseStrict(jsonStr);
		if ((mailObj=mailVal.isObject()) == null) {
			throw new SMFEMailException("Invalid JSON String.");
		}
		//Set message ID
		if ((json_msgid = mailObj.get("id").isString()) != null) {
			this.msg_id = json_msgid.stringValue();
		}
		else
			throw new SMFEMailException("Invalid Message (Message ID not found).");
		//Set message timestamp.
		if ((tmpVal = mailObj.get("timestamp")) !=null 
				&& (json_ts = tmpVal.isNumber()) != null) {
			this.msgts = Long.parseLong(json_ts.toString());
		}
		//Set sender.
		if ((tmpVal = mailObj.get("sender_addr")) !=null 
				&& (json_sender = tmpVal.isString()) != null){
			this.sndrAddr = json_sender.stringValue();
		}
		else
			throw new SMFEMailException("Invalid Message (Sender address not found).");
		//Set message_handle
		if ((tmpVal = mailObj.get("handle")) != null
				&& (json_handle = tmpVal.isString()) != null){
			this.msgHandle = json_handle.stringValue();
		}
		else
			throw new SMFEMailException("Invalid Message (Message handle not found).");
		//Set subject
		if ((tmpVal = mailObj.get("subject")) != null
				&& (json_subject = tmpVal.isString()) != null){
			this.subject = SMFEJsonRpc.fromBase64(json_subject.stringValue());
		}
		else
			this.subject = "";
		//Set body
		if ((tmpVal = mailObj.get("body")) != null
				&& (json_body = tmpVal.isString()) != null) {
			this.body = SMFEJsonRpc.fromBase64(json_body.stringValue());
		}
		else
			this.body = "";
		//Set isRead
		if ((tmpVal = mailObj.get("is_read")) != null
				&& (json_read = tmpVal.isBoolean()) != null) {
			this.isRead = json_read.booleanValue();
		}
		else
			this.isRead = true;
		//Set hasAttacements.
		if ((tmpVal = mailObj.get("has_attachments")) != null
				&& (json_has_attachements = tmpVal.isBoolean()) != null) {
			this.hasAttachements = json_has_attachements.booleanValue();
		}
		else
			this.hasAttachements = false;
		//Set recipients list.
		if ((tmpVal = mailObj.get("receiver_addrs")) != null
				&& (rcptArray = tmpVal.isArray()) != null) {
			int rcptLen = rcptArray.size();
			this.rcptAddrs = new String[rcptLen];
			JSONString mailAddr = null;
			for (int i=0; i<rcptLen; i++) {
				if ((tmpVal = rcptArray.get(i)) != null
						&& (mailAddr = tmpVal.isString()) != null) {
					this.rcptAddrs[i] = mailAddr.stringValue();
				}
				else
					this.rcptAddrs[i] = "";
			}
		}
		//Set CC list.
		if ((tmpVal = mailObj.get("cc_addrs")) != null
				&& (ccArray = tmpVal.isArray()) != null) {
			int ccLen = ccArray.size();
			this.ccAddrs = new String[ccLen];
			JSONString mailAddr = null;
			for (int i=0; i<ccLen; i++) {
				if ((tmpVal = ccArray.get(i)) != null
						&& (mailAddr = tmpVal.isString()) != null) {
					this.ccAddrs[i] = mailAddr.stringValue();
				}
				else
					this.ccAddrs[i] = "";
			}
		}
		//Set BCC list.
		if ((tmpVal = mailObj.get("bcc_addrs")) != null
				&& (bccArray = tmpVal.isArray()) != null) {
			int bccLen = bccArray.size();
			this.bccAddrs = new String[bccLen];
			JSONString mailAddr = null;
			for (int i=0; i<bccLen; i++) {
				if ((tmpVal = bccArray.get(i)) != null
						&& (mailAddr = tmpVal.isString()) != null) {
					this.bccAddrs[i] = mailAddr.stringValue();
				}
				else
					this.bccAddrs[i] = "";
			}
		}
		//Set attachments list
		if ((tmpVal = mailObj.get("attachment_names")) != null
				&& (attachArray = tmpVal.isArray()) != null) {
			int attLen = attachArray.size();
			this.attachements = new String[attLen];
			JSONString mailAddr = null;
			for (int i=0; i<attLen; i++) {
				if ((tmpVal = attachArray.get(i)) != null
						&& (mailAddr = tmpVal.isString()) != null) {
					this.attachements[i] = mailAddr.stringValue();
				}
				else
					this.attachements[i] = "";
			}
		}
	}
	
	public boolean hasAttachements() {
		return hasAttachements;
	}

	public boolean isRead() {
		return isRead;
	}

	public String getHandle() {
		return this.msgHandle;
	}

	public String getMsg_id() {
		return msg_id;
	}

	public String[] getCcAddrs() {
		return ccAddrs;
	}

	public String[] getBccAddrs() {
		return bccAddrs;
	}

	public String getSubject() {
		return subject;
	}

	public String getMsgHandle() {
		return msgHandle;
	}

	public String getSndrAddr() {
		return sndrAddr;
	}

	public String[] getRcptAddrs() {
		return rcptAddrs;
	}

	public long getMsgts() {
		return msgts;
	}
	
}

