package edu.princeton.cs.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.VerticalPanel;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class SyndicateMailFE implements EntryPoint {
	/**
	 * This is the entry point method.
	 */
	public void onModuleLoad() {
		loadUI();
	}
	
	private void loadUI() {
		VerticalPanel mainPanel = new VerticalPanel();
		int mainPanelLen = com.google.gwt.user.client.Window.getClientWidth();
		mainPanel.setWidth(new Integer(mainPanelLen).toString()+"px");
		FlexTable mainTbl = new FlexTable();
		mainTbl.getElement().setClassName("mainTbl");
		mainTbl.setCellPadding(5);
		mainTbl.getColumnFormatter().setWidth(0, "10%");
		mainTbl.getColumnFormatter().setWidth(1, "85%");
		
		SMFMailDir smfInbox = new SMFMailDir((int)(mainPanelLen * 0.8), 
											SMFMailDir.INBOX_ID, mainTbl);
		smfInbox.loadDir();
		
		SMFMailDir smfOutbox = new SMFMailDir((int)(mainPanelLen * 0.8), 
											SMFMailDir.OUTBOX_ID, mainTbl);
		smfOutbox.loadDir();
		
		SMFMailDir[] dirs = new SMFMailDir[2];
		dirs[SMFMailDir.INBOX_ID] = smfInbox;
		dirs[SMFMailDir.OUTBOX_ID] = smfOutbox;
		
		SMFDirList dirList = new SMFDirList((int)(mainPanelLen * 0.1), dirs, SMFMailDir.INBOX_ID);
		VerticalPanel dirListPanel = dirList.loadDirList();
		mainTbl.setWidget(0, 0, dirListPanel);
	
		
		//create SMFDMailComposer
		SMFDMailComposer comp = new SMFDMailComposer();
		
		mainPanel.add(comp.getComposeButton());
		mainPanel.add(mainTbl);
		RootPanel.get().add(mainPanel);
	}
}
