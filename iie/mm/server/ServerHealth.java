package iie.mm.server;

import iie.mm.tools.MM2SSMigrater;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.TreeSet;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.LMDBException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * ServerHealth contains the following module:
 * 
 * 0. monitor info memory used_memory to trigger auto SS migration
 * 1. check and clean mm.dedup.info;
 * 2. check and fix under/over replicated objects;
 * 3. auto clean block file if there are on reference on it; 
 *
 * @author macan
 *
 */
public class ServerHealth extends TimerTask {
	private ServerConf conf;
	private long lastFetch = System.currentTimeMillis();
	private Jedis jedis = null;
	private long used_memory = 0;
	private boolean isMigrating = false;
	private boolean isCleaningDI = false;
	private boolean isFixingObj = false;
	private int nhours = 1;
	
	public static class SetInfo {
		int usedBlocks;
		int totalBlocks;
		long usedLength;
		long totalLength;
		
		public SetInfo() {
			usedBlocks = 0;
			totalBlocks = 0;
			usedLength = 0;
			totalLength = 0;
		}
	}
	
	public static Map<String, SetInfo> setInfos = new HashMap<String, SetInfo>();
	
	public ServerHealth(ServerConf conf) {
		super();
		this.conf = conf;
	}

	@Override
	public void run() {
		try {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String s = df.format(new Date());
			int err = 0;
			
			// fetch used_memory every run
			long cur = System.currentTimeMillis();

			if (cur - lastFetch >= conf.getMemCheckInterval()) {
				try {
					if (jedis == null)
						jedis = new RedisFactory(conf).getDefaultInstance();
					if (jedis == null)
						System.out.println(s + " get redis connection failed.");
					else {
						String info = jedis.info("memory");
						
						if (info != null) {
							String lines[] = info.split("\r\n");
							
							if (lines.length >= 9) {
								String used_mem[] = lines[1].split(":");
								
								if (used_mem.length == 2 && 
										used_mem[0].equalsIgnoreCase("used_memory")) {
									used_memory = Long.parseLong(used_mem[1]);
								}
							}
						}
					}
				} catch (Exception e) {
					jedis = RedisFactory.putBrokenInstance(jedis);
				} finally {
					jedis = RedisFactory.putInstance(jedis);
				}
				lastFetch = cur;
			}
			if (conf.isSSMaster() && 
					used_memory > conf.getMemorySize() * conf.getMemFullRatio()) {
				System.out.println(s + " detect used_memory=" + used_memory +
						" > " + conf.getMemFullRatio() + "*" + conf.getMemorySize());
				
				if (!isMigrating && conf.isEnableSSMig()) {
					isMigrating = true;
					err = SSMigrate(s + " [SSMigrate]");
					if (err < 0) {
						System.out.println(s + " migrate to SS failed w/ " + err);
					} else {
						System.out.println(s + " migrate to SS " + err + " entries.");
					}
					isMigrating = false;
				}
				if (!isCleaningDI) {
					isCleaningDI = true;
					// NOTE: user can set cleanDedupInfo arg(iter) here
					err = cleanDedupInfo(s + " [cleanDedupInfo]", 0);
					if (err < 0) {
						System.out.println(s + " clean dedupinfo failed w/ " + err);
					} else {
						System.out.println(s + " clean dedupinfo " + err + " entries.");
					}
					isCleaningDI = false;
				}
				if (!isFixingObj) {
					isFixingObj = true;
					isFixingObj = false;
				}
			}
			if (true) {
				scrubSets();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * do set migrate, return NR of this set
	 * @param set
	 * @return
	 * @throws Exception
	 */
	private int migrateSet(String set) throws Exception {
		Jedis jedis = new RedisFactory(conf).getDefaultInstance();
		int err = 0, nr = 0;

		if (jedis == null) {
			throw new Exception("Could not get avaliable Jedis instance.");
		}

		try {
			// Save all the server info for this set
			Iterator<String> ir = jedis.smembers(set + ".srvs").iterator();
			String info;
			int idx = 0;

			while (ir.hasNext()) {
				info = ir.next();
				LMDBInterface.getLmdb().write("S#" + set + ".srvs$", info);
				idx++;
			}
			if (idx > 0)
				LMDBInterface.getLmdb().write("S#" + set + ".srvs~size", "" + idx);
			jedis.del(set + ".srvs");

			// Save all the block info for this set
			Iterator<String> ikeys1 = jedis.keys(set + ".blk.*").iterator();

			while (ikeys1.hasNext()) {
				String key1 = ikeys1.next();
				try {
					String value = jedis.get(key1);
					LMDBInterface.getLmdb().write("B#" + key1, value);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			ikeys1 = jedis.keys(set + ".blk.*").iterator();
			Pipeline pipeline1 = jedis.pipelined();

			while (ikeys1.hasNext()) {
				String key1 = ikeys1.next();
				pipeline1.del(key1);
			}
			pipeline1.sync();

			// Save the entries of this set
			Map<String, String> hall = jedis.hgetAll(set);
			nr = hall.size();
			for (Map.Entry<String, String> entry : hall.entrySet()) {
				LMDBInterface.getLmdb().write("H#" + set + "." + entry.getKey(), 
						entry.getValue());
			}
			jedis.del(set);
		} catch (LMDBException le) {
			System.out.println("LMDBException: " + le.getLocalizedMessage());
		} catch (JedisException je) { 
			err = -1;
		} finally {
			if (err < 0)
				jedis = RedisFactory.putBrokenInstance(jedis);
			else
				jedis = RedisFactory.putInstance(jedis);
		}

		return nr;
	}
	
	/**
	 * recycleSet() cleans < ckpt_ts sets (if existed) 
	 */
	private int recycleSet(int up2xhour) throws Exception {
		DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Jedis jedis = new RedisFactory(conf).getDefaultInstance();
		long ckpt_ts = ServerConf.getCkpt_ts();
		int err = 0, nr = 0;
		
		if (jedis == null)
			throw new Exception("Could not get avaliable Jedis instance.");
		
		// get all sets
		TreeSet<String> temp = new TreeSet<String>();
		try {
			Set<String> keys = jedis.keys("*.blk.*");
			
			if (keys != null && keys.size() > 0) {
				String[] keya = keys.toArray(new String[0]);
				
				for (int i = 0; i < keya.length; i++) {
					String set = keya[i].split("\\.")[0];
					
					if (Character.isDigit(set.charAt(0))) {
						temp.add(set);
					} else {
						temp.add(set.substring(1));
					}
				}
			}
			
			String[] prefixs = new String[]{"", "i", "t", "a", "v", "o", "s"};
			
			for (String set : temp) {
				try {
					long thistime = Long.parseLong(set);
					
					if (ckpt_ts < 0)
						ckpt_ts = thistime;
					
					if (ckpt_ts + up2xhour * 3600 >= thistime) {
						// ok, delete it
						System.out.println("Migrate Set:\t" + set + "\t" + 
								df2.format(new Date(thistime * 1000)));
						
						for (String prefix : prefixs) {
							try {
								System.out.println("\t" + prefix + set + "\t" + migrateSet(prefix + set));
								
							} catch (Exception e) {
								System.out.println("MEE: on set " + prefix + set + "," + e.getMessage());
							}
						}
						// ok, update ckpt timestamp: if > ts, check redis, else check lmdb.
						String saved = jedis.get("mm.ckpt.ts");
						long saved_ts = 0, this_ts = 0;
						if (saved != null) {
							try {
								saved_ts = Long.parseLong(saved);
							} catch (Exception e) {
							}
						}
						try {
							this_ts = Long.parseLong(set);
						} catch (Exception e) {
						}
						if (this_ts > saved_ts) {
							jedis.set("mm.ckpt.ts", set);
							ServerConf.setCkpt_ts(this_ts);
						}
						System.out.println("saved=" + saved_ts + ", this=" + this_ts);
						nr++;
					}
				} catch (NumberFormatException nfe) {
					System.out.println("NFE: on set " + set);
				}
			}
		} catch (JedisConnectionException e) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
			err = -1;
			temp.clear();
		} catch (Exception e) {
			e.printStackTrace();
			err = -1;
			temp.clear();
		} finally {
			if (err < 0)
				RedisFactory.putBrokenInstance(jedis);
			else
				RedisFactory.putInstance(jedis);
		}
		
		return nr;
	}
	
	private int SSMigrate(String lh) {
		String line = lh;
		int nr, err = 0;
		
		if (ServerConf.getCkpt_ts() >= 0)
			line += " try to migrate sets that <= " + 
					(nhours * 3600 + ServerConf.getCkpt_ts());
		else
			line += " try to migrate sets from ZERO";

		try {
			nr = recycleSet(nhours);
			if (nr <= 0)
				nhours += 2;
			else
				nhours = 1;
			line += ", mignr = " + nr + " sets.";
		} catch (Exception e) {
			e.printStackTrace();
			err = -1;
		}
		
		System.out.println(line);
		
		return err;
	}

	private int cleanDedupInfo(String lh, long xiter) {
		int kdays = conf.getDi_keep_days();
		long cday = System.currentTimeMillis() / 86400000 * 86400;
		long bTs;
		long iter = 300000, j = 0;
		int deleted = 0;

		if (xiter > 0) iter = xiter;
		bTs = cday - (kdays * 86400);
		System.out.println(lh + " keep day time is " +
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(bTs * 1000)));
		
		try {
			if (jedis == null)
				jedis = new RedisFactory(conf).getDefaultInstance();
			if (jedis == null) {
				System.out.println(lh + " get jedis connection failed.");
				deleted = -1;
			} else {
				Map<String, String> infos = new HashMap<String, String>();
				ScanParams sp = new ScanParams();
				sp.match("*");
				boolean isDone = false;
				String cursor = ScanParams.SCAN_POINTER_START;
				
				while (!isDone) {
					ScanResult<Entry<String, String>> r = jedis.hscan("mm.dedup.info", cursor, sp);
					for (Entry<String, String> entry : r.getResult()) {
						infos.put(entry.getKey(), entry.getValue());
					}
					cursor = r.getStringCursor();
					if (cursor.equalsIgnoreCase("0")) {
						isDone = true;
					}
					j++;
					if (j > iter)
						break;
				}
				
				if (infos != null && infos.size() > 0) {
					for (Map.Entry<String, String> entry : infos.entrySet()) {
						String[] k = entry.getKey().split("@");
						long ts = -1;
						
						if (k.length == 2) {
							try {
								if (Character.isDigit(k[0].charAt(0))) {
									ts = Long.parseLong(k[0]);
								} else {
									ts = Long.parseLong(k[0].substring(1));
								}
							} catch (Exception e) {
								// ignore it
								System.out.println("Ignore set '" + k[0] + "'.");
							}
						}
						if (ts >= 0 && ts < bTs) {
							jedis.hdel("mm.dedup.info", entry.getKey());
							deleted++;
						}
					}
				}
			}
		} catch (Exception e) {
			jedis = RedisFactory.putBrokenInstance(jedis);
		} finally {
			jedis = RedisFactory.putInstance(jedis);
		}
		
		return deleted;
	}

	private int scrubSets() throws Exception {
		Jedis jedis = new RedisFactory(conf).getDefaultInstance();
		int err = 0;

		// get all sets
		TreeSet<String> sets = new TreeSet<String>();
		try {
			Set<String> keys = jedis.keys("*.blk.*");

			if (keys != null && keys.size() > 0) {
				String[] keya = keys.toArray(new String[0]);

				for (int i = 0; i < keya.length; i++) {
					String set = keya[i].split("\\.")[0];

					sets.add(set);
				}
			}
		} catch (JedisConnectionException e) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
			err = -1;
			sets.clear();
		} catch (Exception e) {
			e.printStackTrace();
			err = -1;
			sets.clear();
		} finally {
			if (err < 0)
				RedisFactory.putBrokenInstance(jedis);
			else
				RedisFactory.putInstance(jedis);
		}

		// scrub for each set
		for (String set : sets) {
			err = scrubSetData(set);
		}

		return err;
	}

	private class BlockRef {
		public long ref;
		public long len;

		public BlockRef() {
			ref = 0;
			len = 0;
		}

		public void updateRef(long refDelta, long lenDelta) {
			ref += refDelta;
			len += lenDelta;
		}
	}

	private int scrubSetData(String set) throws Exception {
		int err = 0;

		try {
			if (jedis == null)
				jedis = new RedisFactory(conf).getDefaultInstance();
			if (jedis == null) {
				System.out.println("get jedis connection failed for " + set + " scrub.");
			} else {
				// Map: disk.block -> refcount
				Map<String, BlockRef> br = new HashMap<String, BlockRef>();
				Set<String> disks = new TreeSet<String>();
				ScanParams sp = new ScanParams();
				boolean isDone = false;
				String cursor = ScanParams.SCAN_POINTER_START;

				sp.match("*");
				while (!isDone) {
					ScanResult<Entry<String, String>> r = jedis.hscan(set, cursor, sp);

					for (Entry<String, String> entry : r.getResult()) {
						// parse value into hashmap
						String[] infos = entry.getValue().split("#");

						if (infos != null) {
							for (int i = 0; i < infos.length; i++) {
								String[] vf = infos[i].split("@");
								String disk = null, block = null, serverId = null;
								BlockRef ref = null;
								long len = 0;

								if (vf != null) {
									for (int j = 0; j < vf.length; j++) {
										switch (j) {
										case 1:
											break;
										case 2:
											// serverId
											serverId = vf[j];
											break;
										case 3:
											// blockId
											block = vf[j];
											break;
										case 5:
											// length
											try { 
												len = Long.parseLong(vf[j]);
											} catch (Exception nfe) {};
											break;
										case 6:
											// diskId
											disk = vf[j];
											break;
										}
									}
									try {
										if (serverId != null && disk != null && block != null &&
												Long.parseLong(serverId) == ServerConf.serverId) {
											ref = br.get(disk + "." + block);
											if (ref == null) {
												ref = new BlockRef();
											}
											ref.updateRef(1, len);
											br.put(disk + "." + block, ref);
											if (!disks.contains(disk))
												disks.add(disk);
										}
									} catch (Exception nfe) {}
								}
							}
						}
					}
					cursor = r.getStringCursor();
					if (cursor.equalsIgnoreCase("0")) {
						isDone = true;
					}
				}

				if (!conf.getStoreArray().containsAll(disks)) {
					System.out.println("Got disks set isn't filled in configed store array (" +
						disks + " vs " + conf.getStoreArray() + "), retain it!");
					disks.retainAll(conf.getStoreArray());
				}
				for (String d : disks) {
					int blockMax = 0;
					int usedBlocks = 0;
					long usedLength = 0, totalLength = 0;

					// find the max block id for this disk
					String _blk = jedis.get(set + ".blk." + conf.getNodeName() + "." + d);
					try {
						blockMax = Integer.parseInt(_blk);
					} catch (Exception nfe) {}
					for (int i = 0; i <= blockMax; i++) {
						if (br.get(d + "." + i) == null) {
							if (fexist(d + "/" + conf.destRoot + set + "/b" + i)) {
								if (i < blockMax) {
									if (conf.isVerbose(2))
										System.out.println("Clean block file b" + i + " in disk [" 
												+ d + "] set [" + set + "] ...");
									delFile(new File(d + "/" + conf.destRoot + set + "/b" + i));
								}
							}
						} else {
							usedBlocks++;
							if (fexist(d + "/" + conf.destRoot + set + "/b" + i)) {
								totalLength += statFile(new File(d + "/" + conf.destRoot + set + "/b" + i));
								usedLength += br.get(d + "." + i).len;
								if (conf.isVerbose(2))
									System.out.println("Used  block file b" + i + " in disk [" + 
											d + "] set [" + set + "], ref="	+ br.get(d + "." + i).ref);
							} else {
								System.out.println("Set [" + String.format("%8s", set) + 
										"] in disk [" + d + "] used but not exist.");
							}
						}
					}
					SetInfo si = setInfos.get(set);
					if (si == null) {
						si = new SetInfo();
					}
					si.usedBlocks = usedBlocks;
					si.totalBlocks = blockMax + 1;
					si.usedLength = usedLength;
					si.totalLength = totalLength;
					setInfos.put(set, si);
					if (conf.isVerbose(1))
						System.out.println("Set [" + String.format("%8s", set) + 
								"] in disk [" + d + "] u/t=" + 
								usedBlocks + "/" + (blockMax + 1) + " B.UR=" + 
								String.format("%.4f", (blockMax == 0 ? 0 : (double)usedBlocks / blockMax)) +
								" P.UR=" + 
								String.format("%.4f", (totalLength == 0 ? 0 : (double)usedLength / totalLength)));
				}
			}
		} catch (JedisException e) {
			jedis = RedisFactory.putBrokenInstance(jedis);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			jedis = RedisFactory.putInstance(jedis);
		}

		return err;
	}

	private boolean fexist(String fpath) {
		return new File(fpath).exists();
	}

	private long statFile(File f) {
		if (!f.exists())
			return 0;
		if (f.isFile())
			return f.length();

		return 0;
	}

	private void delFile(File f) {
		if (!f.exists())
			return;
		if (f.isFile())
			f.delete();
		else {
			for (File a : f.listFiles())
				if (a.isFile())
					a.delete();
				else
					delFile(a);
			f.delete();
		}
	}
}
