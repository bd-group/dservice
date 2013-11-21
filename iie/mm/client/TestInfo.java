package iie.mm.client;

import java.io.IOException;
import java.net.UnknownHostException;

import iie.mm.client.*;


public class TestInfo {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ClientConf conf = null;
		try {
			conf = new ClientConf("localhost", 11111, "localhost", 6379, ClientConf.MODE.NODEDUP,1);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(0);
		}
		ClientAPI pcInfo = null;
		try {
			pcInfo = new ClientAPI(conf);
			pcInfo.init("localhost"+"#"+6379);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		byte[] content = new byte[7];
		for(int i=0;i<content.length;i++){
			content[i] = 7;
		}
		pcInfo.put("dsds#md5", content);
		
		byte[] a = pcInfo.get("dsds#md5");
		System.out.println(a.length);
	}

}
