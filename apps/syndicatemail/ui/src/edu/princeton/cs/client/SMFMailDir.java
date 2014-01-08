package edu.princeton.cs.client;

import java.util.Date;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.CellPanel;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTMLTable;
import com.google.gwt.user.client.ui.Hidden;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;

import edu.princeton.cs.shared.SMFEMail;
import edu.princeton.cs.shared.SMFListMessagesAsync;
import edu.princeton.cs.shared.SMFEMailManager;
import edu.princeton.cs.shared.SMFReadMessageAsync;

public class SMFMailDir {
	
	class SMFDirPage {
		
		int len;
		SMFMailDir mailDir;
		
		public SMFDirPage(int len, SMFMailDir mailDir) {
			this.len = len;
			this.mailDir = mailDir;
		}
		
		public void getSMFBoxPage(int type) {
			SMFEMailManager mm = SMFEMailManager.getMailManager();
			try {
				SMFListMessagesAsync mailDirAsync = new SMFListMessagesAsync(mailDir);
				mm.listMessages(type, -1, 20, mailDirAsync);
			}
			catch (RequestException e) {
				Window.alert("Error occured when loading mail directory "+e.getMessage());
			}
		}
	}

	public final static int MAIL_HANDLE_COL = 0;
	public final static int MAIL_SELECT_COL = 1;
	public final static int MAIL_SENDER_COL = 2;
	public final static int MAIL_SUBJECT_COL = 3;
	public final static int MAIL_TIME_COL = 4;
	public final static int MAIL_BOX_TYPE_COL = 5;
	public final static int MAIL_ATT_COL = 6;
	
	public final static int INBOX_PAGE_LEN = 20;
	
	public final static String UI_MODE = "dir_list";
	
	private int panelWidth;
	private FlexTable boxTbl;
	private FlexTable parentTbl;
	private HorizontalPanel dirPanel;
	private int type;
	HandlerRegistration boxTblHR;
	private Hidden mode;
	private Hidden listType;
	private SMFDirPage smfPage;
	private boolean loadPanel;
	
	public SMFMailDir(int width, int type, FlexTable parent) {
		panelWidth = width;
		boxTbl = null;
		this.type = type;
		this.parentTbl = parent;
		dirPanel = new HorizontalPanel();
		
		mode = new Hidden();
		mode.getElement().setId("mode");
		mode.setValue(UI_MODE);
		dirPanel.add(mode);
		
		listType = new Hidden();
		listType.getElement().setId("type");
		listType.setValue(Integer.toString(type));
		dirPanel.add(listType);
		
		this.loadPanel = true;
		
		smfPage = new SMFDirPage(INBOX_PAGE_LEN, this);
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
		boxTbl.getColumnFormatter().setWidth(MAIL_HANDLE_COL, "0%");
		boxTbl.getColumnFormatter().setWidth(MAIL_SELECT_COL, "5%");
		boxTbl.getColumnFormatter().setWidth(MAIL_SENDER_COL, "15%");
		boxTbl.getColumnFormatter().setWidth(MAIL_SUBJECT_COL, "65%");
		boxTbl.getColumnFormatter().setWidth(MAIL_TIME_COL, "10%");
		boxTbl.getColumnFormatter().setWidth(MAIL_ATT_COL, "5%");
		boxTbl.getColumnFormatter().setWidth(MAIL_BOX_TYPE_COL, "0%");
		final SMFMailDir dir = this;
		boxTblHR = boxTbl.addClickHandler(new ClickHandler() {
			@Override
			public void onClick(ClickEvent event) {
				HTMLTable.Cell cell = boxTbl.getCellForEvent(event);
				int cellIndex = cell.getCellIndex();
				if (cellIndex == MAIL_SELECT_COL)
					return;
				try {
					int rowIdex = cell.getRowIndex();
					Hidden hiddenHandle = (Hidden)boxTbl.getWidget(rowIdex, MAIL_HANDLE_COL);
					String handle = hiddenHandle.getValue();
					SMFEMailManager mm = SMFEMailManager.getMailManager();
					SMFReadMessageAsync dispAsync = new SMFReadMessageAsync(dir);
					mm.readMessage(type, handle, dispAsync);
				} catch (RequestException e) {
					Window.alert("Error occured when loading email" + e.getMessage());
				}
			}
		});
		smfPage.getSMFBoxPage(this.type);
	}
	
	public void reloadDir() {
		parentTbl.setWidget(0, 1, dirPanel);
	}
	
	public void loadMailDisplayCallback(SMFEMail mail) {
		SMFMailDisplay smfDisplay = new SMFMailDisplay(mail.getSndrAddr(), mail.getRcptAddrs(), 
				mail.getCcAddrs(), mail.getBccAddrs(), mail.getSubject(), mail.getBody(), 
				mail.getMsgts(), mail.getMsgHandle(), mail.getAttachements(), type);
		CellPanel dspPanel = smfDisplay.getPanel();
		parentTbl.setWidget(0, 1, dspPanel);
	}
	
	public void reloadDirFromStorage() {
		smfPage.getSMFBoxPage(this.type);
		if (loadPanel)
			reloadDir();
	}
	
	public void setLoadPanel(boolean loadPanel) {
		this.loadPanel = loadPanel;
	}
	
	public void removeRow(int i) {
		boxTbl.removeRow(i);
	}
	
	public void loadDirCallback(SMFEMail[] page) {
		for (int i = 0; i < page.length; i++) {
			Hidden mailHandle = new Hidden();
			mailHandle.setValue(page[i].getHandle());
			boxTbl.setWidget(i, MAIL_HANDLE_COL, mailHandle);
			CheckBox chkBox = new CheckBox();
			chkBox.getElement().setClassName("box-item-checkbox");
			boxTbl.setWidget(i, MAIL_SELECT_COL, chkBox);
			//Set sender email in 2nd column.
			Label lbl = new Label(page[i].getSndrAddr());
			if (page[i].isRead()) {
				lbl.setStyleName("mail-read-font");
			}
			boxTbl.setWidget(i, MAIL_SENDER_COL, lbl);
			//Set subject email in 3rd column.
			lbl = new Label(page[i].getSubject());
			if (page[i].isRead()) {
				lbl.setStyleName("mail-read-font");
			}
			boxTbl.setWidget(i, MAIL_SUBJECT_COL, lbl);
			//Set sent time in 4th column
			lbl = new Label(getDisplayDate(page[i].getMsgts()));
			if (page[i].isRead()) {
				lbl.setStyleName("mail-read-font");
			}
			boxTbl.setWidget(i, MAIL_TIME_COL, lbl);
			//Set mail attachment indicator
			Label attLbl = new Label();
			boxTbl.setWidget(i, MAIL_ATT_COL, attLbl);
			if (page[i].hasAttachements()) {
				attLbl.setStyleName("attach-indicator-lbl");
			}
			else {
				attLbl = new Label();
			}
		}
		dirPanel.add(boxTbl);
	}
	
	public FlexTable getDirFlexTable() {
		return boxTbl;
	}
	
	
	private String getDisplayDate(long ts) {
		Date date = new Date(ts * 1000L);
		Date current = new Date();
		String toa = null;
		if (DateTimeFormat.getFormat("y").format(date).equals(DateTimeFormat.getFormat("y").format(current))) {
			if (DateTimeFormat.getFormat("d").format(date).equals(DateTimeFormat.getFormat("d").format(current))) {
				toa = DateTimeFormat.getFormat("h:mm a").format(date);
			}
			else {
				toa = DateTimeFormat.getFormat("MMM d").format(date);
			}
		}
		else {
			toa = DateTimeFormat.getFormat("M/d/yy").format(date);
		}
		return toa;
	}
}
