package edu.princeton.cs.shared;

import com.google.gwt.json.client.JSONObject;
import com.google.gwt.json.client.JSONParser;
import com.google.gwt.user.client.Window;

/**
 * This class encodes/decodes Messages in JSON format that goes through
 * SMFEStorageConnector 
 * @author wathsala
 *
 */
public class SMFEJsonRpc {
	
	private final static String json_rpc_id     = "{\"id\": \"";
	private final static String json_rpc_method = "\", \"method\": \"";
	private final static String json_rpc_args   = "\", \"params\": {\"args\": [";
	private final static String json_rpc_kw		= "],\"kw\": {";
	private final static String json_rpc_version= "}},\"jsonrpc\": \"1.0\"}";
	
	private SMFEJsonRpc() {}
	
	
	private static native String btoa(String str) /*-{return btoa(str);}-*/;
	private static native String escape(String str) /*-{return escape(str);}-*/;
	private static native String decodeURIComponent(String str)/*-{return decodeURIComponent(str);}-*/;
		
	public static String toBase64(String str) {
		return decodeURIComponent(escape(btoa( str )));
	}
	
	public static JSONObject decodeRespose(String jsonStr) {
		JSONObject job = (JSONObject)JSONParser.parseStrict(jsonStr);
		return job;
	}
	
	public static String encodeRPC(String method, String[] args, String[][] kw) {
		//Build args string...
		int len = 0;
		String args_str = "";
		if (args != null) {
			len = args.length;
			for (int i=0; i<len; i++) {
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
}
