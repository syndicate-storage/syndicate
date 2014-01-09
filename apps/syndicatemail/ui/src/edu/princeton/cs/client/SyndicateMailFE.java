package edu.princeton.cs.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.VerticalPanel;

import edu.princeton.cs.shared.SMFEMailManager;
import edu.princeton.cs.shared.SMFEStorageConnector;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class SyndicateMailFE implements EntryPoint {
	/**
	 * This is the entry point method.
	 */
	private final int DEFAULT_MAIL_BOX = SMFEMailManager.INBOX_ID;

	public void onModuleLoad() {
	    loadUI();
	}
	
	private void loadUI() {
		RootPanel.get().setStyleName("body-theme");
		VerticalPanel mainPanel = new VerticalPanel();
		mainPanel.setStyleName("mainTbl");
		HorizontalPanel mainCtrlPanel = new HorizontalPanel();
		mainCtrlPanel.setWidth("92%");
		int mainPanelLen = com.google.gwt.user.client.Window.getClientWidth();
		mainPanel.setWidth(new Integer(mainPanelLen).toString()+"px");
		FlexTable mainTbl = new FlexTable();
		mainTbl.setStyleName("mainTbl");
		mainTbl.setCellPadding(5);
		mainTbl.getColumnFormatter().setWidth(0, "10%");
		mainTbl.getColumnFormatter().setWidth(1, "85%");
		
		SMFMailDir smfInbox = new SMFMailDir((int)(mainPanelLen * 0.8), 
				SMFEMailManager.INBOX_ID, mainTbl);
		smfInbox.loadDir();
		
		SMFMailDir smfOutbox = new SMFMailDir((int)(mainPanelLen * 0.8), 
				SMFEMailManager.OUTBOX_ID, mainTbl);
		smfOutbox.loadDir();
		
		SMFMailDir[] dirs = new SMFMailDir[2];
		dirs[SMFEMailManager.INBOX_ID] = smfInbox;
		dirs[SMFEMailManager.OUTBOX_ID] = smfOutbox;
		
		//Render default mail box
		dirs[DEFAULT_MAIL_BOX].renderDir();
		
		SMFDirList dirList = new SMFDirList((int)(mainPanelLen * 0.1), dirs, SMFEMailManager.INBOX_ID);
		VerticalPanel dirListPanel = dirList.loadDirList();
		mainTbl.setWidget(0, 0, dirListPanel);
	
		
		//create SMFDMailComposer
		SMFDMailComposer comp = new SMFDMailComposer();
		
		mainCtrlPanel.add(comp.getComposeButton());
		mainCtrlPanel.add(dirList.getMailDeleteButton());
		mainCtrlPanel.setStyleName("glass-theme");
		mainPanel.add(mainCtrlPanel);
		mainPanel.add(mainTbl);
		RootPanel.get().add(mainPanel);
	}
}
