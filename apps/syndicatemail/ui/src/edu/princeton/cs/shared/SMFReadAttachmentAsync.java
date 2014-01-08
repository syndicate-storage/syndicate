package edu.princeton.cs.shared;

import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.Response;
import com.google.gwt.user.client.Window;

import edu.princeton.cs.client.SMFMailDisplay;
import edu.princeton.cs.shared.SMFEJsonRpc.SMFEJsonRpcException;

public class SMFReadAttachmentAsync implements RequestCallback{

	private SMFMailDisplay.AttachmentHandler attHandler;
	
	public SMFReadAttachmentAsync(SMFMailDisplay.AttachmentHandler attHandler) {
		this.attHandler = attHandler;
	}
	
	@Override
	public void onResponseReceived(Request request, Response response) {
		/*if (response.getStatusCode() != Response.SC_OK) {
			Window.alert("Error processing request! ["+response.getStatusCode()+"].");
			return;
		}*/
		String jsonStr = "{\"result\": {\"data\":\"HRLcllvo56zbcAJUueudZI5a11X8fcf4Xs+FzbWiW7O4fdFyGgLp3py"
				+ "k5Hnt1tt5YaTAygwZgReHXYrg00pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNK"
				+ "U0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU"
				+ "0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU0pTSlNKU0"
				+ "pTSlNKU0pTSlU4vU4FwxtUcdmqRrdTvFLIDeQkmi75V/DM6+6WReNXaLyMWPEsEUi"
				+ "naKIGTMQFCCVQRONwUQQeY2qmUQR21BmpjP69UpxvM2lknToWYgmVfa\"}}";
		try {
			Window.alert(">>>>>> "+jsonStr);
			jsonStr = SMFEJsonRpc.decodeRespose(jsonStr).toString();
			attHandler.setData(SMFEJsonRpc.fromBase64(jsonStr).getBytes());
		} catch (SMFEJsonRpcException e) {
			Window.alert(e.getMessage());
		}
	}

	@Override
	public void onError(Request request, Throwable exception) {
		
	}

}
