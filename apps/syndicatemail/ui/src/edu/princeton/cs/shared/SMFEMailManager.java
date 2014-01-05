package edu.princeton.cs.shared;

import com.google.gwt.user.client.Window;

/**
 * This is the mail management interface for reading, listing, sending and deleting
 * of mail.
 * @author wathsala
 *
 */
public final class SMFEMailManager {
	
	private static SMFEMailManager singletonMailManager = null;
	private SMFEStorageConnector storageCon;
	
	public static final int INBOX_ID = 0;
	public static final int OUTBOX_ID = 1;
	public static final int TRASH_ID = 2;
	
	protected SMFEMailManager() {
		
	}
	
	public static SMFEMailManager getMailManager () {
		if (singletonMailManager != null)
			return singletonMailManager;
		return new SMFEMailManager();
	}
	
	public SMFEMail readMessage(int dirId, String msgHandle) {
		SMFEMail mail = null;
		return mail;
	}
	
	public void sendMessage(String[] rcptAddrs, String msgBody, String[] attachments) {
		String[] args = new String[2];
		args[0] = SMFEJsonRpc.encode(rcptAddrs);
		args[1] = SMFEJsonRpc.toBase64(msgBody);
		String[][] kw = null;
		if (attachments != null) {
			kw = new String[attachments.length][2];
			for (int i=0; i<attachments.length; i++) {
				kw[i][0] = "Attachment-"+i;
				kw[i][1] = SMFEJsonRpc.toBase64(attachments[i]);
			}
		}
		String jsonStr = SMFEJsonRpc.encodeRPC("send_message", args, kw);
		Window.alert(jsonStr);
		//Read the response from the server and and alert user...
	}
	
	public void deleteMessage(int dirId, String msgHandle) {
		
	}
	
	public SMFEMail[] listMessages(int dirId, long tsStart, int length) {
		SMFEMail[] mailList = null;
		
		/*Pack dirID and tsStart to String array (args)*/
		
		String[] args = new String[2];
		args[0] = dirId2FolderName(dirId);
		args[1] = ""+tsStart;
		
		String[][] kw = new String[1][2];
		kw[0][0] = "length";
		kw[0][1] =  ""+length;
		String jsonStr = SMFEJsonRpc.encodeRPC("send_message", args, kw);
		Window.alert(jsonStr);
		//Read the response from the server encode them to the an array of 
		//SMFEMail instances.
		return mailList;
	}
	
	private String dirId2FolderName(int dirId){
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
