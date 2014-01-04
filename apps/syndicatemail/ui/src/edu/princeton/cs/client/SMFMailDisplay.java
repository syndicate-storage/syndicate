package edu.princeton.cs.client;

import com.google.gwt.user.client.ui.CellPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.VerticalPanel;

public class SMFMailDisplay {

	String sender;
	String subject;
	String body;
	long ts;
	VerticalPanel dspPanel;
	
	public SMFMailDisplay(String sender, String subject, String body, long ts) {
		this.sender = sender;
		this.subject = subject;
		this.body = body;
		this.ts = ts;
		loadDisplay();
	}
	
	private void loadDisplay() {
		if (dspPanel != null)
			return;
		dspPanel = new VerticalPanel();
		dspPanel.getElement().setClassName("mail-display-panel");
		dspPanel.setWidth("70%");
		//Add subject label...
		Label sbjLbl = new Label();
		sbjLbl.setStyleName("mail-display-sbjlbl");
		sbjLbl.setText(this.subject);
		dspPanel.add(sbjLbl);
		//Add sender label...
		Label sndrLbl = new Label();
		sndrLbl.setStyleName("mail-display-sndrlbl");
		sndrLbl.setText(this.sender);
		dspPanel.add(sndrLbl);
		//Add email body...
		TextArea body = new TextArea();
		body.setStyleName("mail-display-body");
		body.setWidth("100%");
		body.setHeight("500px");
		body.setText(this.body);
		body.setEnabled(false);
		dspPanel.add(body);
	}
	
	public CellPanel getPanle() {
		return dspPanel;
	}
}
