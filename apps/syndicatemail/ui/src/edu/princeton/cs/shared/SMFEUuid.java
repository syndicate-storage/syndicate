package edu.princeton.cs.shared;

import com.google.gwt.user.client.Random;

public class SMFEUuid {
	
	private int uuid_0;
	private short uuid_1;
	private short uuid_2;
	private short uuid_3;
	private short[] uuid_4;
	private String uuid;
	
	public SMFEUuid() {
		uuid = null;
		int MSB_int_mask = ~(1<<31);
		//Generate the first 8-bytes of the UUID
		uuid_0 = Random.nextInt() & MSB_int_mask;

		int MSB_short_mask = ~(1<<15);
		//Generate the 2nd 4-bytes of the UUID
		uuid_1 = (short)Random.nextInt();
		uuid_1 &= MSB_short_mask;

		//Generate the 3rd 4-bytes of the UUID
		uuid_2 = (short)Random.nextInt();
		uuid_2 &= MSB_short_mask;

		//Generate the 4th 4-bytes of the UUID
		uuid_3 = (short)Random.nextInt();
		uuid_3 &= MSB_short_mask;

		//Generate the 5th 12-bytes of the UUID
		uuid_4 = new short[3];
		uuid_4[0] = (short)Random.nextInt();
		uuid_4[0] &= MSB_short_mask;
		uuid_4[1] = (short)Random.nextInt();
		uuid_4[1] &= MSB_short_mask;
		uuid_4[2] = (short)Random.nextInt();
		uuid_4[2] &= MSB_short_mask;
	}
	
	public String toString() {
		if (uuid != null)
			return uuid;
		int len = 0;
		String uuid_0_str = Integer.toHexString(uuid_0);
		if ((len = uuid_0_str.length()) < 8) {
			String tmp_uuid_0 = uuid_0_str;
			for (int i=0; i<len; i++) {
				tmp_uuid_0 = "0"+uuid_0_str;
			}
			uuid_0_str = tmp_uuid_0;
		}
		String uuid_1_str = Integer.toHexString(uuid_1);
		if ((len = uuid_1_str.length()) < 4) {
			String tmp_uuid_1 = uuid_1_str;
			for (int i=0; i<len; i++) {
				tmp_uuid_1 = "0"+uuid_1_str;
			}
			uuid_1_str = tmp_uuid_1;
		}
		String uuid_2_str = Integer.toHexString(uuid_2);
		if ((len = uuid_2_str.length()) < 4) {
			String tmp_uuid_2 = uuid_2_str;
			for (int i=0; i<len; i++) {
				tmp_uuid_2 = "0"+uuid_2_str;
			}
			uuid_2_str = tmp_uuid_2;
		}
		String uuid_3_str = "";
		byte val = 0;
		for (int i=0; i<4; i++) {
			int mask = 0xf << (i*4);
			val = (byte)((uuid_3 & mask) >> (i*4));
			uuid_3_str += Integer.toHexString(val);
		}
		String uuid_4_str = "";
		for (int i=0; i<3; i++) {
			for (int j=0; j<4; j++) {
				int mask = 0xf << (j*4);
				val = (byte)((uuid_4[i] & mask) >> (j*4));
				uuid_4_str += Integer.toHexString(val);
			}
		}
		uuid = uuid_0_str+"-"+uuid_1_str+"-"+uuid_2_str+
				"-"+uuid_3_str+"-"+uuid_4_str;
		return uuid;

	}
}
