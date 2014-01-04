package edu.princeton.cs.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CellPanel;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.VerticalPanel;

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
	private FlexTable boxTbl;
	private FlexTable parentTbl;
	private HorizontalPanel dirPanel;
	private int type;
	private static Button deleteMailBtn;
	HandlerRegistration boxTblHR;
	
	private final int INBOX_PAGE_LEN = 20;
	public static final int INBOX_ID = 0;
	public static final int OUTBOX_ID = 1;
	
	
	public SMFMailDir(int width, int type, FlexTable parent) {
		panelWidth = width;
		boxTbl = null;
		this.type = type;
		this.parentTbl = parent;
		dirPanel = new HorizontalPanel();
	}
	
	public static Button getDeleteButton(final FlexTable parent) {
		deleteMailBtn = new Button();
		deleteMailBtn.setText("Delete");
		deleteMailBtn.getElement().setClassName("delete-button");
		deleteMailBtn.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				HorizontalPanel chkPanel = (HorizontalPanel)parent.getWidget(0, 1);
				FlexTable dirTbl = (FlexTable)chkPanel.getWidget(0);
				for (int i=0; i<dirTbl.getRowCount(); i++) {
					CheckBox cb = (CheckBox)dirTbl.getWidget(i, 0);
					if (cb.getValue()) {
						Window.alert(i+" is checked!");
						dirTbl.removeRow(i);
						i--;
						//TODO: Add an older mail to the end of the list
					}
				}
			}
		});
		return deleteMailBtn;
	}
	
	public void renderDir() {
		parentTbl.setWidget(0, 1, dirPanel);
	}
	
	public void loadDir() {
		if (boxTbl != null)
			return;
		//Create a 3x20 table
		boxTbl = new FlexTable();
		//boxPanel.setWidth(new Integer(panelWidth).toString()+"px");
		boxTbl.setWidth(new Integer(panelWidth).toString()+"px");
		boxTbl.setCellPadding(8);
		boxTbl.getElement().setClassName("inboxTbl");
		boxTbl.getColumnFormatter().setWidth(0, "5%");
		boxTbl.getColumnFormatter().setWidth(1, "15%");
		boxTbl.getColumnFormatter().setWidth(2, "70%");
		boxTbl.getColumnFormatter().setWidth(3, "10%");
		boxTblHR = boxTbl.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				int cellIndex = boxTbl.getCellForEvent(event).getCellIndex();
				if (cellIndex == 0)
					return;
				int rowIndex = boxTbl.getCellForEvent(event).getRowIndex();
				String sender = boxTbl.getCellFormatter().getElement(rowIndex, 1).getInnerText();
				String subject = boxTbl.getCellFormatter().getElement(rowIndex, 2).getInnerText();
				String time = boxTbl.getCellFormatter().getElement(rowIndex, 3).getInnerText();
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
		parentTbl.setWidget(0, 1, dirPanel);
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
					CheckBox chkBox = new CheckBox();
					chkBox.getElement().setClassName("box-item-checkbox");
					boxTbl.setWidget(i, 0, chkBox);
					boxTbl.setWidget(i, 1, new Label(inpage[i][0]));
					boxTbl.setWidget(i, 2, new Label(inpage[i][1]));
					boxTbl.setWidget(i, 3, new Label(inpage[i][2]));
				}
				break;
			case OUTBOX_ID:
				String[][] outpage = smfPage.getSMFOutboxPage();
				for (int i = 0; i < 20; i++) {
					CheckBox chkBox = new CheckBox();
					chkBox.getElement().setClassName("box-item-checkbox");
					boxTbl.setWidget(i, 0, chkBox);
					boxTbl.setWidget(i, 1, new Label(outpage[i][0]));
					boxTbl.setWidget(i, 2, new Label(outpage[i][1]));
					boxTbl.setWidget(i, 3, new Label(outpage[i][2]));				}
				break;
		}
		dirPanel.add(boxTbl);
	}
}
