package edu.princeton.cs.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CellPanel;
import com.google.gwt.user.client.ui.Hidden;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.VerticalPanel;

import edu.princeton.cs.shared.SMFEMailManager;
import edu.princeton.cs.shared.SMFReadAttachmentAsync;

public class SMFMailDisplay {

	public static final String MAIL_HANDLE_ID = "mail_handle";
	private final String sender;
	private final String subject;
	private final String body;
	private final long ts;
	private VerticalPanel dspPanel;
	private final String[] rcptAddrs, ccAddrs, bccAddrs, attachements;
	private final String mailHandle;
	private int type;
	public final static String UI_MODE = "mail_display";
	private final int DEFAULT_SUBJECT_LEN  = 25;
	private final int DEFAULT_ATT_NAME_LEN = 20;
	
	public class AttachmentHandler {
		
		private String attachment_handle;
		private String attachment_name;
		private byte[] data;
		
		public AttachmentHandler(String att_name, String att_handle) {
			this.attachment_handle = att_handle;
			this.attachment_name = att_name;
		}
		
		public void setData(byte[] data) {
			this.data = data;
		}
		
		public byte[] getData() {
			return this.data;
		}
	}
	
	public SMFMailDisplay(String sender, String[] rcptAddrs, String[] ccAddrs, String[] bccAddrs,
			String subject, String body, long ts, String handle, String[] attachements, int type) {
		this.sender = sender;
		this.subject = subject;
		this.body = body;
		this.ts = ts;
		this.rcptAddrs = rcptAddrs;
		this.ccAddrs = ccAddrs;
		this.bccAddrs = bccAddrs;
		this.attachements = attachements;
		this.mailHandle = handle;
		this.type = type;
		loadDisplay();
	}
	
	private String StrArraytoString(String[] array) {
		String str = "";
		for (int i=0; i<array.length; i++) {
			str += array[i];
			if (i+1 < array.length)
				str += ",";
		}
		return str;
	}
	
	private void loadDisplay() {
		if (dspPanel != null)
			return;
		dspPanel = new VerticalPanel();
		dspPanel.getElement().setId(UI_MODE);
		dspPanel.setStyleName("mail-display-panel");
		dspPanel.setWidth("70%");
		//Add subject label...
		Label sbjLbl = new Label();
		sbjLbl.setStyleName("mail-display-sbjlbl");
		String dspSbj = null;
		if (this.subject.length() > DEFAULT_SUBJECT_LEN)
			dspSbj = this.subject.substring(0, DEFAULT_SUBJECT_LEN).trim() + "...";
		else
			dspSbj = this.subject;
		sbjLbl.setText(dspSbj);
		dspPanel.add(sbjLbl);
		
		
		// Put sender label in a separate panel.
		VerticalPanel metaDataPanel = new VerticalPanel();
		metaDataPanel.getElement().setClassName("mail-display-meta-panel");
		
		// Add sender label...
		VerticalPanel sndrPanel = new VerticalPanel();
		Label sndrLbl = new Label();
		sndrLbl.setStyleName("mail-display-sndrlbl");
		sndrLbl.setText(this.sender);
		sndrPanel.add(sndrLbl);
		metaDataPanel.add(sndrPanel);
		
		//email addresses panel holds addresses of other recipients, cc and bcc lists.
		final VerticalPanel mailAddrListPanel = new VerticalPanel();
		mailAddrListPanel.setWidth("100%");
		mailAddrListPanel.setVisible(false);
		//Add rcpts panel
		if (rcptAddrs != null && rcptAddrs.length > 0) {
			HorizontalPanel rcptPanel = new HorizontalPanel();
			rcptPanel.setWidth("100%");
			rcptPanel.setStyleName("mail-display-rcpt-panel");
			Label recvListLbl = new Label("Sent to: ");
			recvListLbl.setStyleName("mail-display-rcptlbl");
			rcptPanel.add(recvListLbl);
			Label recvList = new Label();
			recvList.setText(StrArraytoString(rcptAddrs));
			recvList.setStyleName("mail-display-rcptlist");
			recvList.setWidth("100%");
			rcptPanel.add(recvList);
			mailAddrListPanel.add(rcptPanel);
		}
		
		//Add CC panel
		if (ccAddrs != null && ccAddrs.length > 0) {
			HorizontalPanel ccPanel = new HorizontalPanel();
			ccPanel.setWidth("100%");
			ccPanel.setStyleName("mail-display-rcpt-panel");
			Label ccListLbl = new Label("CCed to: ");
			ccListLbl.setStyleName("mail-display-rcptlbl");
			ccPanel.add(ccListLbl);
			Label ccTxt = new Label();
			ccTxt.setText(StrArraytoString(ccAddrs));
			ccTxt.setStyleName("mail-display-rcptlist");
			ccTxt.setWidth("100%");
			ccPanel.add(ccTxt);
			mailAddrListPanel.add(ccPanel);
		}
		
		//Add BCC panel
		if (bccAddrs != null && bccAddrs.length > 0) {
			HorizontalPanel bccPanel = new HorizontalPanel();
			bccPanel.setWidth("100%");
			bccPanel.setStyleName("mail-display-rcpt-panel");
			Label bccListLbl = new Label("BCCed to: ");
			bccListLbl.setStyleName("mail-display-rcptlbl");
			bccPanel.add(bccListLbl);
			Label bccTxt = new Label();
			bccTxt.setText(StrArraytoString(bccAddrs));
			bccTxt.setStyleName("mail-display-rcptlist");
			bccTxt.setWidth("100%");
			bccPanel.add(bccTxt);
			mailAddrListPanel.add(bccPanel);
		}
		
		//Add a clickable text so that email address panel could be opened and closed
		final Label showMailList = new Label();
		showMailList.setText("More");
		showMailList.setStyleName("maild-display-show-addrs");
		showMailList.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				mailAddrListPanel.setVisible(true);
				showMailList.setVisible(false);
			}
		});
		
		mailAddrListPanel.addDomHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				mailAddrListPanel.setVisible(false);
				showMailList.setVisible(true);
			}
		}, ClickEvent.getType());
		metaDataPanel.add(showMailList);
		metaDataPanel.add(mailAddrListPanel);

		//Put Reply button in a separate panel.
		HorizontalPanel mailActionsPanel = new HorizontalPanel();
		mailActionsPanel.getElement().setClassName("mail-display-action-panel");
		
		//Put Attachment icon and attachmentListPanel list in a vertial panel.AttachmentListPanel is a 
		//Vertical panel holding all the attachment links.
		//Add Attachment indicator.
		//AttachPanel
		if (attachements != null) {
			VerticalPanel attachmentPanel = new VerticalPanel();
			attachmentPanel.setWidth("100%");
			final VerticalPanel attachmentListPanel = new VerticalPanel();
			attachmentListPanel.setVisible(false);
			final Label attLbl = new Label();
			attLbl.setStyleName("mail-display-attach-indicator-lbl");
			attLbl.addClickHandler(new ClickHandler() {
				private boolean styleToggle = true;
				@Override
				public void onClick(ClickEvent event) {
					if (styleToggle) {
						attLbl.setStyleName("mail-display-attach-indicator-lbl-min");
						attachmentListPanel.setVisible(true);
					}
					else {
						attLbl.setStyleName("mail-display-attach-indicator-lbl");
						attachmentListPanel.setVisible(false);
					}
					styleToggle = !styleToggle;
				}
			});
			attachmentPanel.add(attLbl);
			int len = attachements.length;
			Label[] attList = new Label[len];
			for (int i = 0; i < len; i++) {
				attList[i] = new Label();
				if (attachements[i].length() > DEFAULT_ATT_NAME_LEN)
					attList[i].setText(attachements[i].substring(0, DEFAULT_ATT_NAME_LEN-3)+"...");
				else
					attList[i].setText(attachements[i]);
				attList[i].setStyleName("mail-display-attachment");
				final String attName = attachements[i];
				final String attHandle = attachements[i];
				attList[i].addClickHandler(new ClickHandler() {
					@Override
					public void onClick(ClickEvent event) {
						AttachmentHandler attHandler = new AttachmentHandler(attName, attHandle);
						SMFReadAttachmentAsync attHandlerAsync = new SMFReadAttachmentAsync(attHandler);
						SMFEMailManager mm = SMFEMailManager.getMailManager();
						try {
							mm.readAttachment(attName, attHandlerAsync);
						} catch (RequestException e) {
							Window.alert("Failed downloading attachment: "+attName);
						}
					}
				});
				attachmentListPanel.add(attList[i]);
			}
			attachmentPanel.add(attachmentListPanel);
			mailActionsPanel.add(attachmentPanel);
		}

		//Add reply button
		Button replyBtn = new Button();
		replyBtn.getElement().setClassName("mail-display-reply-btn");
		replyBtn.setText("Reply");
		replyBtn.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				new SMFDMailComposer().loadReplySendDialog(sender, rcptAddrs, ccAddrs,bccAddrs, 
													subject, body, ts, mailHandle, attachements );
			}
		});
		mailActionsPanel.add(replyBtn);
		
		//Put metaDataPanel and mailActionPanel in mailControlPanel.
		HorizontalPanel mailControPanel = new HorizontalPanel();
		mailControPanel.getElement().setClassName("mail-display-control-panel");
		mailControPanel.add(metaDataPanel);
		mailControPanel.add(mailActionsPanel);
		//Put mailControlPanel in dspPanel.
		dspPanel.add(mailControPanel);
		
		TextArea body = new TextArea();
		body.setStyleName("mail-display-body");
		body.setText(this.body);
		body.setEnabled(false);
		dspPanel.add(body);
		
		//Add hidden field for handle
		Hidden hidden = new Hidden();
		hidden.getElement().setId(MAIL_HANDLE_ID);
		hidden.setValue(this.mailHandle);
		dspPanel.add(hidden);
		//Add hidden element to store UI_MODE
		Hidden mode = new Hidden();
		mode.getElement().setId("mode");
		mode.setValue(UI_MODE);
		dspPanel.add(mode);
		// Add hidden element to store UI_MODE
		Hidden dirType = new Hidden();
		dirType.getElement().setId("type");
		dirType.setValue(new Integer(type).toString());
		dspPanel.add(dirType);
	}
	
	public CellPanel getPanel() {
		return dspPanel;
	}
}
