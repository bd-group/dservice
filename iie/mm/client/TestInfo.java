package iie.mm.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
		
//		byte[] content = new byte[8];
//		for(int i=0;i<content.length;i++){
//			content[i] = 7;
//		}
//		pcInfo.put("dsds#md5", content);
		
		pcInfo.iGet("1#zzz#zhuqihan-pc#11111#0#0#6477#.");
		Set<String> s = new HashSet<String>();
		s.add("1#zzz#zhuqihan-pc#11111#0#0#6477#.");
		Map<String, byte[]>  a = pcInfo.wait(s);
		for(String str : a.keySet()){
			System.out.println(a.get(str));
		}
		
	}

}
