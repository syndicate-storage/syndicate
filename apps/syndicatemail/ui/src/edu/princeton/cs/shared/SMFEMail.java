package edu.princeton.cs.shared;

/**
 * This is simple class to represent a single Email.
 * @author wathsala
 *
 */
public class SMFEMail {
	
	public String[] getAttachements() {
		return attachements;
	}

	public String getBody() {
		return body;
	}

	private String msgHandle;
	private String sndrAddr;
	private String[] rcptAddrs;
	private String[] attachements;
	private String body;
	private long msgts;
	
	public SMFEMail(String msgHandle, String sndrAddr, String[] rcptAddrs, String body, String attachments[], long msgts) {
		this.msgHandle = msgHandle;
		this.sndrAddr = sndrAddr;
		this.rcptAddrs = rcptAddrs;
		this.msgts = msgts;
	}
	
	public String getHandle() {
		return this.msgHandle;
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

