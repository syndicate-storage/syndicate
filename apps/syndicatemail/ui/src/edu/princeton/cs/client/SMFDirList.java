package edu.princeton.cs.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.VerticalPanel;

import edu.princeton.cs.shared.*;

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
}
