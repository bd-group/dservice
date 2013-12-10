package iie.mm.client;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;


public class TestInfo {

	/**
	 * @param args
	 * @throws Exception 
	 */
	/*
	public static void main(String[] args) throws Exception {
		ClientConf conf = null;
		try {
			conf = new ClientConf("192.168.1.239", 11111, "192.168.1.239", 6379, ClientConf.MODE.NODEDUP,1);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(0);
		}
		ClientAPI pcInfo = null;
		try {
			pcInfo = new ClientAPI(conf);
			pcInfo.init("192.168.1.239"+":"+6379);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		byte[][] content = new byte[2][];
		FileInputStream fis = new FileInputStream("cry.gif");
		content[0] = new byte[fis.available()];
		fis.read(content[0]);
		fis = new FileInputStream("abc.jpeg");
		content[1] = new byte[fis.available()];
		fis.read(content[1]);
		String[] md5s = {"imput1","imput2"};
	//	pcInfo.mPut("mPut",md5s, content);
//		pcInfo.iPut("iput@iputtest", content);
		Jedis jedis = new Jedis(conf.getRedisHost(),conf.getRedisPort());
		List<String> vals = jedis.hvals("mPut");
//		String string = "mput@psb2.gif";
//		pcInfo.iGet(string);
		Set<String> s = new HashSet<String>();
		s.addAll(vals);
		pcInfo.imGet(s);
		Map<String, byte[]> a = pcInfo.wait(s);
		FileOutputStream fos = null;
		for(String str : a.keySet()){
			fos = new FileOutputStream(str);
			fos.write(a.get(str));
			fos.close();
		}
		
	}
	*/
}
