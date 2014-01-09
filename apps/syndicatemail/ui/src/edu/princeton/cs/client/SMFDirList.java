package edu.princeton.cs.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Element;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.Hidden;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.VerticalPanel;

import edu.princeton.cs.shared.SMFDeleteMessageAsync;
import edu.princeton.cs.shared.SMFEMailManager;

public class SMFDirList {
	private int panelWidth;
	private VerticalPanel dirListPanel;
	private SMFMailDir[] dirs;
	
	
    private int activeDir;
    public SMFDirList(int width, SMFMailDir[] dirs, int activeDir) {
		panelWidth = width;
		dirListPanel = null;
		this.dirs = dirs;
		this.activeDir = activeDir; 
	}
	
	private void setActiveDir(int id) {
		activeDir = id;
	}
	
	protected SMFMailDir getActiveDir() {
		return dirs[activeDir];
	}
	
	public VerticalPanel loadDirList() {
		if (dirListPanel != null)
			return dirListPanel;
		dirListPanel = new VerticalPanel();
		dirListPanel.setStyleName("dir-list-panel");
		//Create a 1x2 table
		FlexTable dirListTbl = new FlexTable();
		dirListPanel.setWidth(new Integer(panelWidth).toString()+"px");
		dirListTbl.setCellPadding(5);
		Label inboxLbl = new Label("Inbox");
		inboxLbl.getElement().setClassName("inbox_dir_lbl");
		dirListTbl.setWidget(0, 0, inboxLbl);
		
		Label outboxLbl = new Label("Outbox");
		outboxLbl.getElement().setClassName("outbox_dir_lbl");
		dirListTbl.setWidget(1, 0, outboxLbl);
		
		//Install event handlers...
		inboxLbl.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				dirs[SMFEMailManager.INBOX_ID].reloadDir();
				setActiveDir(SMFEMailManager.INBOX_ID);
			}
		});
		outboxLbl.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				dirs[SMFEMailManager.OUTBOX_ID].reloadDir();
				setActiveDir(SMFEMailManager.OUTBOX_ID);
			}
		});
		
		dirListPanel.add(dirListTbl);
		RootPanel.get().add(dirListPanel);
		return dirListPanel;
	}
	
	public Button getMailDeleteButton() {
		if (dirs == null)
			return null;
		Button deleteMailBtn = new Button();
		deleteMailBtn.setText("Delete");
		deleteMailBtn.getElement().setClassName("delete-button");
		deleteMailBtn.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				Element element = DOM.getElementById("mode");
				if (element == null)
					return;
				String mode = element.getAttribute("value");
				if (mode == null)
					return;
				element = DOM.getElementById("type");
				if (element == null)
					return;
				String typeStr = element.getAttribute("value");
				if (typeStr == null)
					return;
				int dirType = Integer.parseInt(typeStr);
				SMFEMailManager mm = SMFEMailManager.getMailManager();
				if (mode.equals(SMFMailDir.UI_MODE)) {
					FlexTable dirTbl = dirs[dirType].getDirFlexTable();
					for (int i = 0; i < dirTbl.getRowCount(); i++) {
						CheckBox cb = (CheckBox) dirTbl.getWidget(i,
								SMFMailDir.MAIL_SELECT_COL);
						if (cb.getValue()) {
							Hidden hiddenFld = (Hidden) dirTbl.getWidget(i,
									SMFMailDir.MAIL_HANDLE_COL);
							String mailHandle = hiddenFld.getValue();
							dirs[dirType].setLoadPanel(false);
							SMFDeleteMessageAsync delAsync = 
									new SMFDeleteMessageAsync(dirs[dirType]);
							try {
								mm.deleteMessage(dirType, mailHandle, delAsync);
							}
							catch (RequestException e) {
								Window.alert("Error deleting email: "+e.getMessage());
							}
							dirs[dirType].removeRow(i);
							i--;
						}
					}
				}
				else if (mode.equals(SMFMailDisplay.UI_MODE)) {
					element = DOM.getElementById(SMFMailDisplay.MAIL_HANDLE_ID);
					if (element == null)
						return;
					String mailHandle = element.getAttribute("value");
					dirs[dirType].setLoadPanel(true);
					SMFDeleteMessageAsync delAsync = new SMFDeleteMessageAsync(dirs[dirType]);
					try {
						mm.deleteMessage(dirType, mailHandle, delAsync);
					}
					catch (RequestException e){
						Window.alert("Error deleting email: "+e.getMessage());
					}
				}
				else {
					return;
				}
			}
		});
		return deleteMailBtn;
	}
}
