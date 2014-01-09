package edu.princeton.cs.shared;

import com.google.gwt.json.client.JSONObject;
import com.google.gwt.json.client.JSONParser;
import com.google.gwt.json.client.JSONValue;

/**
 * This class encodes/decodes Messages in JSON format that goes through
 * SMFEStorageConnector 
 * @author wathsala
 *
 */
public class SMFEJsonRpc {
	
	public class SMFEJsonRpcException extends Exception{
		
		private static final long serialVersionUID = 1L;
		private String msg;
		private JSONValue jsonRpcError;
		
		public SMFEJsonRpcException(String msg) {
			this.msg = msg;
			this.jsonRpcError = null;
		}
		
		public String getMessage() {
			return this.msg;
		}
		
		public String toString() {
			return this.msg;
		}
		
		public void setJsonRpcError(JSONValue ev){
			this.jsonRpcError = ev;
		}
		
		public JSONValue getJsonRpcError() {
			return this.jsonRpcError;
		}
	}
	
	private final static String json_rpc_id     = "{\"id\": \"";
	private final static String json_rpc_method = "\", \"method\": \"";
	private final static String json_rpc_args   = "\", \"params\": {\"args\": [";
	private final static String json_rpc_kw		= "],\"kw\": {";
	private final static String json_rpc_version= "}},\"jsonrpc\": \"1.0\"}";
	
	private SMFEJsonRpc() {}
	
	
	private static native String btoa(String str) /*-{
		try {
			return btoa(str);
		}
		catch (e) {
			return str;
		}
	}-*/;
	private static native String atob(String str) /*-{
		try {
			return atob(str);
		}
		catch (e) {
			return str;
		}
	}-*/;
	private static native String escape(String str) /*-{
		return escape(str);
	}-*/;
	private static native String decodeURIComponent(String str)/*-{
		return decodeURIComponent(str);
	}-*/;
	private static native String encodeURIComponent(String str)/*-{
		return encodeURIComponent(str);
	}-*/;

	
	public static String toBase64(String str) {
		return btoa(encodeURIComponent( str ));
	}
	
	public static String fromBase64(String str) {
		return decodeURIComponent(atob( str ));
	}
	
	public static JSONValue decodeRespose(String jsonStr) throws SMFEJsonRpcException{
		SMFEJsonRpc rpc = new SMFEJsonRpc();
		if (jsonStr == null)
			throw rpc.new SMFEJsonRpcException("Invalid Response");
		JSONObject job = (JSONObject)JSONParser.parseStrict(jsonStr);
		if (job == null) 
			throw rpc.new SMFEJsonRpcException("Invalid Response ("+jsonStr+")");
		//Read result.
		JSONValue value = job.get("result");
		if (value == null) {
			//Check for error...
			value = job.get("error");
			if (value == null)
				throw rpc.new SMFEJsonRpcException("Invalid Response ("+jsonStr+")");
			if (value.isNull() == null)
				throw rpc.new SMFEJsonRpcException("A Weird Response ("+jsonStr+")");
			else {
				SMFEJsonRpcException e = rpc.new SMFEJsonRpcException("A Weird Response ("+value.toString()+")");
				e.setJsonRpcError(value);
			}
		}
		if (value.isNull() != null)
			throw rpc.new SMFEJsonRpcException("A Weird Response ("+jsonStr+")");
		else 
			return value;
	}
	
	public static String encodeRPC(String method, String[] args, String[][] kw) {
		//Build args string...
		int len = 0;
		String args_str = "";
		if (args != null) {
			len = args.length;
			for (int i=0; i<len; i++) {
				if (args[i] == null || args[i].equals(""))
					continue;
				if (isNumeric(args[i]))
					args_str += args[i];
				else
					args_str += ("\"" + args[i] + "\"");
				if (i + 1 != len)
					args_str += ",";
			}
		}
		//Build kw string...
		String kw_str = "";
		if (kw != null) {
			len = kw.length;
			for (int i=0; i<len; i++) {
				kw_str += "\"";
				kw_str += kw[i][0];
				kw_str += "\":";
				if (isNumeric(kw[i][1])) {
					kw_str += kw[i][1];
				}
				else
					kw_str += ("\"" + kw[i][1] + "\"");
				if (i + 1 != len)
					kw_str += ",";
			}
		}
		String uuid = new SMFEUuid().toString();
		String jsonStr = json_rpc_id+uuid+json_rpc_method+method+
				json_rpc_args+args_str
				+json_rpc_kw+kw_str
				+json_rpc_version;
		return jsonStr;
	}
	
	public static String encode(String[] args) {
		if (args == null)
			return null;
		//Encode args to a single json string.
		String jsonStr = "[";
		int len = args.length;
		for (int i=0; i<len; i++) {
			if (isNumeric(args[i]))
				jsonStr += args[i];
			else
				jsonStr += ("\"" + args[i] + "\"");
			if (i + 1 != len)
				jsonStr += ",";
		}
		jsonStr += "]";
		return jsonStr;
	}
	
	public static String encode(int[] args) {
		if (args == null)
			return null;
		//Encode args to a single json string.
		String jsonStr = "[";
		int len = args.length;
		for (int i=0; i<len; i++) {
			jsonStr += args[i];
			if (i + 1 != len)
				jsonStr += ",";
		}
		jsonStr += "]";
		return jsonStr;
	}
	
	public static String encode(long[] args) {
		if (args == null)
			return null;
		//Encode args to a single json string.
		String jsonStr = "[";
		int len = args.length;
		for (int i=0; i<len; i++) {
			jsonStr += args[i];
			if (i + 1 != len)
				jsonStr += ",";
		}
		jsonStr += "]";
		return jsonStr;
	}
	
	private static boolean isNumeric(String str)
	{
		if (str.charAt(0) == '[' && str.charAt(str.length() - 1) == ']') {
			return true;
		}
		try  
		  {  
		    double d = Double.parseDouble(str);  
		  }  
		  catch(NumberFormatException nfe)  
		  {  
		    return false;  
		  }  
		  return true; 
	}
	
	public static String toJSONString(String key, String value) {
		if (key == null)
			return null;
		if (value == null)
			value = "";
		String jsonStr = "{\""+key+"\":\""+value+"\"}";
		return jsonStr;
	}
	
	public static String toJSONArray(String[][] array, boolean encodeB64) {
		int len = array.length;
		String jsonStr = "[";
		for (int i=0; i<len; i++) {
			if (encodeB64)
				jsonStr += toJSONString(array[i][0], toBase64(array[i][1]));
			else
				jsonStr += toJSONString(array[i][0], array[i][1]);
			if (i + 1 != len)
				jsonStr += ",";
		}
		jsonStr += "]";
		return jsonStr;
	}

	
	public static boolean getBooleanValue(JSONValue val) throws SMFEJsonRpcException {
		SMFEJsonRpc rpc = new SMFEJsonRpc();
		if (val == null) 
			throw rpc.new SMFEJsonRpcException("Invalid JSON String");
		if (val.isBoolean() != null)
			return val.isBoolean().booleanValue();
		else
			throw rpc.new SMFEJsonRpcException("Not a Boolean Value");
	}
}
