package edu.princeton.cs.client;

import com.google.gwt.event.dom.client.BlurEvent;
import com.google.gwt.event.dom.client.BlurHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CellPanel;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;

public class SMFDMailComposer {
	private Button composeBtn;
	private DialogBox dlgBox;
	private int displayWidth;
	private int displayHeight;
	private int dlgBoxWidth;
	private int dlgBoxHeight;
	
	public SMFDMailComposer() {
		displayWidth = Window.getClientWidth();
		displayHeight = Window.getClientHeight();
		
		dlgBoxWidth = (int)(displayWidth/2.5);
		dlgBoxHeight = (int)(displayHeight/2.5);
		
		composeBtn = new Button();
		composeBtn.setText("COMPOSE");
		composeBtn.getElement().setClassName("compose_button");		
		
		//Setup the dialog box
		dlgBox = new DialogBox();
		dlgBox.setPopupPosition(dlgBoxWidth/2, dlgBoxHeight/2);
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
		//Add a button panel
		CellPanel btnPabel = getButtonPanel();
		dialogPanel.add(btnPabel);
		
		dlgBox.add(dialogPanel);
		
		//Set compose button event handler
		composeBtn.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				// Window.alert("Pop the Compose Window");
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
		final TextBox toTxt = new TextBox();
		toTxt.setWidth(new Integer(dlgBoxWidth).toString()+"px");
		toTxt.setText("To");
		toTxt.addStyleName("rcpt-txt-box");
		toTxt.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				toTxt.setText("");
			}
		});
		toTxt.addBlurHandler(new BlurHandler() {
			@Override
			public void onBlur(BlurEvent event) {
				if (toTxt.getText().equals(""))
					toTxt.setText("To");
			}
		});
		headerPanel.add(toTxt);
		//Add subject text box.
		final TextBox sbjTxt = new TextBox();
		sbjTxt.setWidth(new Integer(dlgBoxWidth).toString()+"px");
		sbjTxt.addStyleName("subject-txt-box");
		sbjTxt.setText("Subject");
		sbjTxt.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				sbjTxt.setText("");
			}
		});
		sbjTxt.addBlurHandler(new BlurHandler() {
			@Override
			public void onBlur(BlurEvent event) {
				if (sbjTxt.getText().equals(""))
					sbjTxt.setText("Subject");
			}
		});
		headerPanel.add(sbjTxt);
		return headerPanel;
	}
	
	public CellPanel getBodyPanel() {
		VerticalPanel bodyPanel = new VerticalPanel();
		bodyPanel.addStyleName("body-panel");
		bodyPanel.setWidth("100%");
		TextArea emailBody = new TextArea();
		emailBody.setStyleName("email-body");
		emailBody.setSize(new Integer(dlgBoxWidth).toString()+"px", 
				new Integer(dlgBoxHeight).toString()+"px");
		bodyPanel.add(emailBody);
		return bodyPanel;
	}
	
	public CellPanel getButtonPanel() {
		HorizontalPanel btnPanel = new HorizontalPanel();
		btnPanel.setWidth("100%");
		btnPanel.addStyleName("button-panel");
		Button sendBtn = new Button("Send");
		sendBtn.getElement().setClassName("send-button");
		sendBtn.addClickHandler(new ClickHandler() {
			
			@Override
			public void onClick(ClickEvent event) {
				Window.alert("Sending your email...");				
			}
		});
		btnPanel.add(sendBtn);
		return btnPanel;
	}
}
