package edu.princeton.cs.shared;

import java.util.Vector;

import com.google.gwt.http.client.RequestException;
import com.google.gwt.json.client.JSONArray;
import com.google.gwt.json.client.JSONParser;
import com.google.gwt.json.client.JSONValue;
import com.google.gwt.user.client.Window;

import edu.princeton.cs.client.SMFMailDir;
import edu.princeton.cs.shared.SMFEJsonRpc.SMFEJsonRpcException;
import edu.princeton.cs.shared.SMFEMail.SMFEMailException;

/**
 * This is the mail management interface for reading, listing, sending and deleting
 * of mail.
 * @author wathsala
 *
 */
public final class SMFEMailManager {
	
	private static SMFEMailManager singletonMailManager = null;
	
	public static final int INBOX_ID = 0;
	public static final int OUTBOX_ID = 1;
	public static final int TRASH_ID = 2;
	
	SMFEStorageConnector storageCon;
	
	protected SMFEMailManager() {
		storageCon = SMFEStorageConnector.getStorageConnector();
	}
	
	public static SMFEMailManager getMailManager () {
		if (singletonMailManager != null)
			return singletonMailManager;
		return new SMFEMailManager();
	}
	
	public void readMessage(int dirId, String msgHandle, SMFReadMessageAsync mailDir) throws RequestException {
		String dir = getFolderNameById(dirId);
		/**Pack dir and msgHandle to a String array (args)*/
		String[] args = new String [2];
		args[0] = dir;
		args[1] = msgHandle;
		//We don't have KW args for this RPC.
		String jsonStr = SMFEJsonRpc.encodeRPC("read_message", args, null);
		Window.alert(jsonStr);
		storageCon.write(jsonStr, mailDir);
	}
	
	public void sendMessage(String[] rcptAddrs, String[] ccAddrs, String[] bccAddrs,
							String subject, String msgBody, String[][] attachments,
							SMFSendMessageAsync sendAsync) throws RequestException {
		String[] args = new String[5];
		args[0] = SMFEJsonRpc.encode(rcptAddrs);
		args[1] = SMFEJsonRpc.encode(ccAddrs);
		args[2] = SMFEJsonRpc.encode(bccAddrs);
		args[3] = SMFEJsonRpc.toBase64(subject);
		args[4] = SMFEJsonRpc.toBase64(msgBody);
		String[][] kw = null;
		if (attachments != null) {
			kw = new String[1][2];
			kw[0][0] = "attachments";
			kw[0][1] = SMFEJsonRpc.toJSONArray(attachments, true);
		}
		String jsonStr = SMFEJsonRpc.encodeRPC("send_message", args, kw);
		Window.alert(jsonStr);
		storageCon.write(jsonStr, sendAsync);
		//Read the response from the server and and alert user...
	}
	
	public void deleteMessage(int dirId, String msgHandle, SMFDeleteMessageAsync delAsync) throws RequestException {
		String dir = getFolderNameById(dirId);
		/** Pack dir and msgHandle to a String array (args) */
		String[] args = new String[2];
		args[0] = dir;
		args[1] = msgHandle;
		// We don't have KW args for this RPC.
		String jsonStr = SMFEJsonRpc
				.encodeRPC("delete_message", args, null);
		Window.alert(jsonStr);
		storageCon.write(jsonStr, delAsync);
	}
	
	public void listMessages(int dirId, long tsStart, int length, 
									SMFListMessagesAsync mailDir) throws RequestException {
		/*Pack dirID and tsStart to String array (args)*/	
		String[] args = new String[2];
		args[0] = getFolderNameById(dirId);
		args[1] = ""+tsStart;
		
		String[][] kw = new String[1][2];
		kw[0][0] = "length";
		kw[0][1] =  ""+length;
		String jsonStr = SMFEJsonRpc.encodeRPC("list_message", args, kw);
		Window.alert(jsonStr);
		storageCon.write(jsonStr, mailDir);
	}
	
	public void readAttachment(String attachment_name, 
								SMFReadAttachmentAsync attHandler) throws RequestException {
		String[] args = new String[1];
		args[0] = attachment_name;
		String jsonStr = SMFEJsonRpc.encodeRPC("read_attachment", null, null);
		Window.alert(jsonStr);
		storageCon.write(jsonStr, attHandler);
	}
		
	private String getFolderNameById(int dirId){
		String folderName = null;
		switch (dirId) {
		case INBOX_ID:
			folderName = "Inbox";
			break;
		case OUTBOX_ID:
			folderName = "Sent";
			break;
		case TRASH_ID:
			folderName = "Trash";
			break;
		default:
			break;
		}
		return folderName;
	}
}
