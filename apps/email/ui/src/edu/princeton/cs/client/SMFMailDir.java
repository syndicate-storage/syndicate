package edu.princeton.cs.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.CellPanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.Label;

public class SMFMailDir {
	
	class SMFDirPage {
		
		int len;
		
		public SMFDirPage(int len) {
			this.len = len;
		}
		
		public String[][] getSMFInboxPage() {
			String[][] page = new String[len][3];
			for (int i=0; i<len; i++) {
				page[i][0] = "wvithanag@gmail.com";
				page[i][1] = "About Syndicate Mail...";
				page[i][2] = "10:45";
			}
			return page;
		}
		
		public String[][] getSMFOutboxPage() {
			String[][] page = new String[len][3];
			for (int i=0; i<len; i++) {
				page[i][0] = "wvithanag@gmail.com";
				page[i][1] = "This is a SENT mail...";
				page[i][2] = "10:45";
			}
			return page;
		}
		
	}
	
	private int panelWidth;
	FlexTable boxTbl;
	FlexTable parentTbl;
	private int type;
	HandlerRegistration boxTblHR;
	
	private final int INBOX_PAGE_LEN = 20;
	public static final int INBOX_ID = 0;
	public static final int OUTBOX_ID = 1;
	
	
	public SMFMailDir(int width, int type, FlexTable parent) {
		panelWidth = width;
		boxTbl = null;
		this.type = type;
		this.parentTbl = parent;
	}
	
	
	public void loadDir() {
		if (boxTbl != null)
			return;
		//Create a 3x20 table
		boxTbl = new FlexTable();;
		//boxPanel.setWidth(new Integer(panelWidth).toString()+"px");
		boxTbl.setWidth(new Integer(panelWidth).toString()+"px");
		boxTbl.setCellPadding(8);
		boxTbl.getElement().setClassName("inboxTbl");
		boxTbl.getColumnFormatter().setWidth(0, "15%");
		boxTbl.getColumnFormatter().setWidth(1, "75%");
		boxTbl.getColumnFormatter().setWidth(2, "10%");
		boxTblHR = boxTbl.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				int rowIndex = boxTbl.getCellForEvent(event).getRowIndex();
				String sender = boxTbl.getCellFormatter().getElement(rowIndex, 0).getInnerText();
				String subject = boxTbl.getCellFormatter().getElement(rowIndex, 1).getInnerText();
				String time = boxTbl.getCellFormatter().getElement(rowIndex, 2).getInnerText();
				loadMailDisplay(sender, subject, "HELLO WORLD .......", 0);
			}
		});
		this._loadDir();
	}
	
	public void reloadDir() {
		//boxTbl.removeAllRows();
		//if (boxTblHR != null)
		//	boxTblHR.removeHandler();
		//this._loadDir();
		parentTbl.setWidget(0, 1, boxTbl);
	}
	
	private void loadMailDisplay(String sender, String subject, String body, long ts) {
		SMFMailDisplay smfDisplay = new SMFMailDisplay(sender, subject, body, ts);
		CellPanel dspPanel = smfDisplay.getPanle();
		parentTbl.setWidget(0, 1, dspPanel);
	}
	
	private void _loadDir() {
		SMFDirPage smfPage = new SMFDirPage(INBOX_PAGE_LEN);
		switch (this.type) {
			case INBOX_ID:
				String[][] inpage = smfPage.getSMFInboxPage();
				for (int i = 0; i < 20; i++) {
					boxTbl.setWidget(i, 0, new Label(inpage[i][0]));
					boxTbl.setWidget(i, 1, new Label(inpage[i][1]));
					boxTbl.setWidget(i, 2, new Label(inpage[i][2]));
					parentTbl.setWidget(0, 1, boxTbl);
				}
				break;
			case OUTBOX_ID:
				String[][] outpage = smfPage.getSMFOutboxPage();
				for (int i = 0; i < 20; i++) {
					boxTbl.setWidget(i, 0, new Label(outpage[i][0]));
					boxTbl.setWidget(i, 1, new Label(outpage[i][1]));
					boxTbl.setWidget(i, 2, new Label(outpage[i][2]));
				}
				break;
		}
	}
}
