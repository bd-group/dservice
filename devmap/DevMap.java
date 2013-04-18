package devmap;

import java.util.HashMap;

import devmap.DevMap.DevStat;

public class DevMap {

	public static class DevStat {
		public String mount_point;
		public long read_nr;
		public long write_nr;
		public long err_nr;
	
		public DevStat(String mp, long read_nr, long write_nr, long err_nr) {
			this.mount_point = mp;
			this.read_nr = read_nr;
			this.write_nr = write_nr;
			this.err_nr = err_nr;
		}
	
		public static DevStat convertToDevStat(String str) {
			String[] ls = str.split(",");
			if (ls.length == 4) {
				return new DevStat(ls[0], Long.parseLong(ls[1]), Long.parseLong(ls[2]), Long.parseLong(ls[3]));
			}
			return null;
		}
	};
	
	private HashMap<String, DevStat> devmap = new HashMap<String, DevStat>();
	
    static { 
    	System.loadLibrary("devmap");
    }

    public static native boolean isValid();

    public static native String getDevMaps();
    
    public DevMap() {
    	refreshDevMap();
    }
    
    public void refreshDevMap() {
    	devmap.clear();
    	String content = getDevMaps();
    	String ls[]	= content.split("\n");
    	for (String line : ls) {
    		String[] r = line.split(":");
    		if (r.length == 2) {
    			DevStat v = DevStat.convertToDevStat(r[1]);
    			if (v != null)
    				devmap.put(r[0], v); 
    		}
    	}
    }
    
    public String dumpDevMap() {
    	String s = "";
    	
    	for (String k : devmap.keySet()) {
    		DevStat v = devmap.get(k);
    		s += "Key " + k + ", Value: " + v.mount_point + "," + v.read_nr + "," + 
    				v.write_nr + "," + v.err_nr + "\n";
    	}
    	
    	return s;
    }
    
    public DevStat findDev(String devid) {
    	return devmap.get(devid);
    }
}

