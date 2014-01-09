package edu.princeton.cs.client;

import java.util.TreeMap;
import java.util.Vector;

import com.google.gwt.event.dom.client.BlurEvent;
import com.google.gwt.event.dom.client.BlurHandler;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.user.client.Element;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CellPanel;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;

import edu.princeton.cs.shared.FieldVerifier;
import edu.princeton.cs.shared.SMFEMailManager;
import edu.princeton.cs.shared.SMFEUuid;
import edu.princeton.cs.shared.SMFSendMessageAsync;

public class SMFDMailComposer {
	private Button composeBtn;
	private DialogBox dlgBox;
	private int displayWidth;
	private int displayHeight;
	private int dlgBoxWidth;
	private int dlgBoxHeight;
	private final TextBox toTxt = new TextBox();
	private final TextBox sbjTxt = new TextBox();
	private final TextBox ccTxt = new TextBox();
	private final TextBox bccTxt = new TextBox();
	private final TextArea emailBody = new TextArea();
	
	private final String defaultSubjct = "Subject";
	private final String defaultTo = "To";
	private final String defaultCc = "Cc";
	private final String defaultBcc = "Bcc";
	private final Vector<String> attachment_handler_list = new Vector<String>();
	private final TreeMap<String, String> attachment_fname_list = new TreeMap<String, String>();
	private final VerticalPanel attachmentsPanel;
	
	public static final native void clickElement(Element elem) /*-{
		elem.click();
	}-*/;
	
	public SMFDMailComposer() {
		displayWidth = Window.getClientWidth();
		displayHeight = Window.getClientHeight();
		
		dlgBoxWidth = (int)(displayWidth/2.5);
		dlgBoxHeight = (int)(displayHeight/2.5);
		
		composeBtn = new Button();
		composeBtn.setText("COMPOSE");
		composeBtn.getElement().setClassName("compose_button");	
		
		//Setup the attachments panel
		attachmentsPanel = new VerticalPanel();
		attachmentsPanel.setWidth("100%");
		attachmentsPanel.setStyleName("attachments-panel");
		
		//Setup the dialog box
		dlgBox = new DialogBox();
		dlgBox.setPopupPosition(dlgBoxWidth/2, 0/*dlgBoxHeight/2*/);
		dlgBox.setGlassEnabled(true);
		dlgBox.setTitle("Compose");
		dlgBox.setStyleName("compose-dlg");	
		
		VerticalPanel dialogPanel = new VerticalPanel();
		//Add title bar to diaglogbox
		CellPanel titlePanel = getTitlePanel();
		dialogPanel.add(titlePanel);
		//Add a header panel
		CellPanel headerPanel = getHeaderPanel();
		dialogPanel.add(headerPanel);
		//Add Text area to contain the email body
		CellPanel emailBodyPanel = getBodyPanel();
		dialogPanel.add(emailBodyPanel);
		//Add attachments panel
		dialogPanel.add(attachmentsPanel);
		
		//Add a button panel
		CellPanel btnPabel = getButtonPanel();
		dialogPanel.add(btnPabel);
		
		dlgBox.add(dialogPanel);
		
		//Set compose button event handler
		composeBtn.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				// Window.alert("Pop the Compose Window");
				resetSendDialog();
				dlgBox.show();
			}
		});
	}
	
	private void dialogClose() {
		dlgBox.hide();
	}
	
	public Button getComposeButton() {
		return composeBtn;
	}
	
	public CellPanel getTitlePanel() {
		HorizontalPanel titlePanel = new HorizontalPanel();
		titlePanel.setWidth("100%");
		titlePanel.addStyleName("compose-title");
		HTML close = new HTML("X");
		close.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				dialogClose();
			}
		});
		HTML title =new HTML("Compose");
		titlePanel.addStyleName("compose-title");
		title.addStyleName("compose-caption");
		titlePanel.add(title);
		close.addStyleName("compose-close-button");
		titlePanel.add(close);
		return titlePanel;
	}
	
	public CellPanel getHeaderPanel() {
		VerticalPanel headerPanel = new VerticalPanel();
		headerPanel.setWidth("100%");
		headerPanel.addStyleName("header-panel");
		//Add to text box.
		initTextBox(toTxt, defaultTo);
		headerPanel.add(toTxt);
		
		//CC box
		initTextBox(ccTxt, defaultCc);
		headerPanel.add(ccTxt);
		
		//BCC box
		initTextBox(bccTxt, defaultBcc);
		headerPanel.add(bccTxt);
		
		//Add subject text box.
		initTextBox(sbjTxt, defaultSubjct);
		headerPanel.add(sbjTxt);
		
		return headerPanel;
	}
	
	private void initTextBox(final TextBox box, final String defaultTxt) {
		box.setWidth(new Integer(dlgBoxWidth).toString()+"px");
		box.addStyleName("rcpt-txt-box");
		//sbjTxt.setText("Subject");
		box.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				if (box.getText().equals(defaultTxt))
					box.setText("");
			}
		});
		box.addBlurHandler(new BlurHandler() {
			@Override
			public void onBlur(BlurEvent event) {
				if (box.getText().equals(""))
					box.setText(defaultTxt);
			}
		});
	}
	
	public CellPanel getBodyPanel() {
		VerticalPanel bodyPanel = new VerticalPanel();
		bodyPanel.addStyleName("body-panel");
		bodyPanel.setWidth("100%");
		emailBody.setStyleName("email-body");
		emailBody.setSize(new Integer(dlgBoxWidth).toString()+"px", 
				new Integer(dlgBoxHeight/2).toString()+"px");
		bodyPanel.add(emailBody);
		return bodyPanel;
	}
	
	public CellPanel getAttachmentsPanel() {
		return this.attachmentsPanel;
	}
	
	public CellPanel getButtonPanel() {		
		final FormPanel form = new FormPanel();
		form.setAction("http://127.0.0.1/cgi-bin/test.sh");
		form.setVisible(false);
		//final VerticalPanel formWidgetPanel = new VerticalPanel();
		//form.add(formWidgetPanel);
		
		final HorizontalPanel btnPanel = new HorizontalPanel();
		btnPanel.setWidth("100%");
		btnPanel.addStyleName("button-panel");
		Button sendBtn = new Button("Send");
		sendBtn.getElement().setClassName("send-button");
		final SMFDMailComposer composer = this;
		sendBtn.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				if (FieldVerifier.getEmailAddrs(toTxt.getText()) == null)
					return;
				SMFEMailManager mm = SMFEMailManager.getMailManager();
				String[] rcptAddrs = FieldVerifier.getEmailAddrs(toTxt.getText());
				String[] bccList = FieldVerifier.getEmailAddrs(bccTxt.getText());
				String[] ccList = FieldVerifier.getEmailAddrs(ccTxt.getText());
				String msgBody = emailBody.getText();
				String subject = sbjTxt.getText();
				String[][] attachments = null;
				SMFSendMessageAsync sendAsync = new SMFSendMessageAsync(composer);
				/**
				 * Loop through all the files and get file names...
				 */
				int nrAttachements = attachment_handler_list.size();
				attachments = new String[nrAttachements][2];
				form.submit();
				for (int i=0; i<nrAttachements; i++) {
					String attachment_handler = attachment_handler_list.get(i);
					String attachment_file_name = attachment_fname_list.get(attachment_handler);
					//Window.alert(name+" -> "+fu.getFilename());
					attachments[i][0] = attachment_handler;
					attachments[i][1] = attachment_file_name;
				}
				attachment_handler_list.removeAllElements();
				attachment_fname_list.clear();
				try {
					if (rcptAddrs.length == 0)
						return;
					mm.sendMessage(rcptAddrs, ccList, bccList, subject, msgBody, attachments, sendAsync);
				} catch (RequestException e) {
					Window.alert("Sending Failed...\nSorry, we don't have a Drafts box yet!\n"+e.getMessage());
					composer.unloadSendDialog();
				}
			}
		});
		btnPanel.add(sendBtn);
		//Add an attach button..
		Button attachBtn = new Button();
		attachBtn.getElement().setClassName("attach-button");
		final VerticalPanel attPanel = (VerticalPanel) getAttachmentsPanel();
		attachBtn.addClickHandler(new ClickHandler() {
			String name = null;
			@Override
			public void onClick(ClickEvent event) {
				//Add a file uploader with a random name...
				name = new SMFEUuid().toString();
				final FileUpload fileUpload = new FileUpload();
				fileUpload.setVisible(false);
				fileUpload.getElement().setId(name);
				fileUpload.addChangeHandler(new ChangeHandler() {
					@Override
					public void onChange(ChangeEvent event) {
						final HorizontalPanel attWidgetPanel = new HorizontalPanel();
						String fileName = getFileName(fileUpload.getFilename());
						attachment_handler_list.add(name);
						attachment_fname_list.put(name, fileName);
						Label attLbl = new Label(fileName);
						attLbl.setStyleName("attchment-lbl");
						Label delAttLbl = new Label("X");
						delAttLbl.setStyleName("delatt-lbl");
						delAttLbl.addClickHandler(new ClickHandler() {
							@Override
							public void onClick(ClickEvent event) {
								attachment_handler_list.remove(name);
								attachment_fname_list.remove(name);
								attPanel.remove(attWidgetPanel);
							}
						});
						attWidgetPanel.setStyleName("attachment-panle");
						attWidgetPanel.add(attLbl);
						attWidgetPanel.add(delAttLbl);
						attPanel.add(attWidgetPanel);
					}
				});
				form.add(fileUpload);
				btnPanel.add(fileUpload);
				clickElement(fileUpload.getElement());
			}
		});
		btnPanel.add(attachBtn);
		btnPanel.add(form);
		return btnPanel;
	}
	
	public String getFileName(String path) {
		if (path == null || path.equals(""))
			return null;
		String fileName = null;
		int endPos = path.length() - 1;
		for (int i=endPos; i>=0; i--) {
			if (path.charAt(i) == '\\') {
				fileName = path.substring(i+1);
				break;
			}
		}
		return fileName;
	}
	
	public void unloadSendDialog() {
		dlgBox.hide();
		resetSendDialog();
	}
	
	public void loadSendDialog() {
		resetSendDialog();
		dlgBox.show();
	}
	
	public void loadReplySendDialog(String sender, String[] rcptAddrs, String[] ccAddrs, String[] bccAddrs,
			String subject, String body, long ts, String handle, String[] attachements) {
		resetSendDialog();
		sbjTxt.setText(subject);
		toTxt.setText(sender);
		emailBody.setText("\n\n\n--------ORIGINAL MESSAGE---------\n\n"+body);
		dlgBox.show();
	}
	
	private void resetSendDialog() {
		resetSbjTxt();
		resetBody();
		resetToTxt();
		resetCcTxt();
		resetBccTxt();
		resetAttachmentsPanel();
	}
	
	private void resetSbjTxt() {
		sbjTxt.setText(defaultSubjct);
	}
	
	private void resetToTxt() {
		toTxt.setText(defaultTo);
	}
	
	private void resetBccTxt() {
		bccTxt.setText(defaultBcc);
	}
	
	private void resetCcTxt() {
		ccTxt.setText(defaultCc);
	}
	
	private void resetBody() {
		emailBody.setText(null);
	}
	
	private void resetAttachmentsPanel() {
		attachmentsPanel.clear();
	}
}
