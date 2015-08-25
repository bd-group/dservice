package iie.mm.server;

import iie.mm.server.HTTPHandler.TopKeySet.KeySetEntry;
import iie.mm.server.ServerHealth.SetInfo;
import iie.mm.server.StorePhoto.ObjectContent;
import iie.mm.server.StorePhoto.RedirectException;
import iie.mm.server.StorePhoto.SetStats;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class HTTPHandler extends AbstractHandler {
	private ServerConf conf;
	private StorePhoto sp;
	
	public HTTPHandler(ServerConf conf) {
		this.conf = conf;
		sp = new StorePhoto(conf);
	}
	
	private void badResponse(Request baseRequest, HttpServletResponse response, String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		baseRequest.setHandled(true);
		response.getWriter().println(message);
		response.getWriter().flush();
	}
	
	private void notFoundResponse(Request baseRequest, HttpServletResponse response, String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_NOT_FOUND);
		baseRequest.setHandled(true);
		response.getWriter().println(message);
		response.getWriter().flush();
	}
	
	private void okResponse(Request baseRequest, HttpServletResponse response, byte[] content) throws IOException {
		// FIXME: text/image/audio/video/application/thumbnail/other
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getOutputStream().write(content);
		response.getOutputStream().flush();
	}
	
	private void redirectResponse(Request baseRequest, HttpServletResponse response, RedirectException e) throws IOException {
		String serverUrl = ServerConf.servers.get(e.serverId);
		response.setContentType("text/plain;charset=utf-8");
		baseRequest.setHandled(true);
		if (serverUrl == null) {
			response.setStatus(HttpServletResponse.SC_NOT_FOUND);
			response.getWriter().println("#FAIL: Redirect to serverId " + e.serverId + " failed.");
		} else {
			String[] url = serverUrl.split(":");
			InetSocketAddress isa = new InetSocketAddress(url[0],666);
			response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
			//response.getWriter().println(serverUrl);
			//response.getWriter().println(e.info);
			response.sendRedirect("http://" + isa.getAddress().getHostAddress()+":"+url[1] + "/get?key=" + e.info);
		}
		response.getWriter().flush();
	}
	
	private void doGet(String target, Request baseRequest, HttpServletRequest request, 
			HttpServletResponse response) throws IOException, ServletException {
		String key = request.getParameter("key");
		
		try {
		if (key == null) {
			response.setContentType("text/html;charset=utf-8");
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			baseRequest.setHandled(true);
			response.getWriter().println("#FAIL: key can not be null");
		} else {
			String[] infos = key.split("@|#");
			
			if (infos.length == 2) {
				ObjectContent oc = null;
				try {
					oc = sp.getPhoto(infos[0], infos[1]);
					if (oc == null || oc.content == null || oc.length == 0) {
						notFoundResponse(baseRequest, response, "#FAIL:can not find any MM object by key=" + key);
					} else {
						switch (infos[0].charAt(0)) {
						case 'i':
						case 's':
							response.setContentType("image");
							break;
						case 't':
							response.setContentType("text/plain;charset=utf-8");
							break;
						case 'a':
							response.setContentType("audio");
							break;
						case 'v':
							response.setContentType("video");
							break;
						default:
							response.setContentType("text/plain;charset=utf-8");
						}
						okResponse(baseRequest, response, Arrays.copyOfRange(oc.content, 0, oc.length));
					}
				} catch (RedirectException e) {
					redirectResponse(baseRequest, response, e);
				}
			} else if (infos.length == 7) {
				ObjectContent oc = null;
				try {
					oc = sp.searchPhoto(key, null, null);
					if (oc == null || oc.content == null || oc.length == 0) {
						notFoundResponse(baseRequest, response, "#FAIL:can not find any MM object by key=" + key);
					} else {
						switch (infos[1].charAt(0)) {
						case 'i':
						case 's':
							response.setContentType("image");
							break;
						case 't':
							response.setContentType("text/plain;charset=utf-8");
							break;
						case 'a':
							response.setContentType("audio");
							break;
						case 'v':
							response.setContentType("video");
							break;
						default:
							response.setContentType("text/plain;charset=utf-8");
						}
						okResponse(baseRequest, response, Arrays.copyOfRange(oc.content, 0, oc.length));
					}
				} catch (RedirectException e) {
					redirectResponse(baseRequest, response, e);
				}
			} else {
				badResponse(baseRequest, response, "#FAIL: invalid key format {" + key + "}");
			}
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void doPut(String target, Request baseRequest, HttpServletRequest request, 
			HttpServletResponse response) throws IOException, ServletException {
		badResponse(baseRequest, response, "#FAIL: not implemented yet.");
	}
	
	private String convertToDate(long ts) {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		if (ts < 0) {
			return "Invalid Time Stamp";
		} else
			return df.format(new Date(ts * 1000));
	}

	private void doInfo(String target, Request baseRequest, HttpServletRequest request, 
			HttpServletResponse response) throws IOException, ServletException {
		Jedis jedis = null;

		try {
			jedis = new RedisFactory(conf).getDefaultInstance();
			response.setContentType("text/html;charset=utf-8");
			response.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			String page = "<HTML> " +
							"<HEAD>" +
							"<TITLE> MM Server Info: [MMS" + ServerConf.serverId + "]</TITLE>" +
							"</HEAD>" +
							"<BODY>" +
							"<H1> #In Current Server Session: </H1>" +
							"<H2> Server Info: [MMS" + ServerConf.serverId + "]</H2><tt>" +
							"Uptime              (S): " + ((System.currentTimeMillis() - PhotoServer.upts) / 1000) + "<p>" +
							"Writes (#): total " + ServerProfile.writeN.longValue() + ", error <font color=\"red\">" + ServerProfile.writeErr.longValue() + "</font><p>" +
							"Reads  (#): total " + ServerProfile.readN.longValue() + ", error <font color=\"red\">" + ServerProfile.readErr.longValue() + "</font><p>" +
							"Indexs (#): queued " + ServerProfile.queuedIndex.longValue() + ", handled " + ServerProfile.handledIndex.longValue() + ", ignored " + ServerProfile.ignoredIndex.longValue() + ", completed " + ServerProfile.completedIndex.longValue() + "</font><p>" +
							"Total Written Bytes (B): " + ServerProfile.writtenBytes.longValue() + "<p>" +
							"Total Read    Bytes (B): " + ServerProfile.readBytes.longValue() + "<p>" +
							"Avg Read Latency   (ms): " + (double)ServerProfile.readDelay.longValue() / ServerProfile.readN.longValue() + "<p>" +
							(conf.isSSMaster() ? ("<p>" + 
							"Secondary First TimeStamp(s): " + LMDBInterface.getLmdb().firstTS() + "\t" + convertToDate(LMDBInterface.getLmdb().firstTS()) + "<p>" + 
							"Secondary Server Keep KV pairs(#): " + LMDBInterface.getLmdb().count() + "<p>" +
							"Checkpoint TimeStamp(s): " + ServerConf.getCkpt_ts() + "\t" + convertToDate(ServerConf.getCkpt_ts())) : "") + "</tt>" +
							PhotoServer.getServerInfoHtml(conf) + "<p>" +
							PhotoServer.getSpaceInfoHtml(conf) + "<p>" +
							"<H1> #Client Auto Config: </H1><tt>" +
							"dupmode = " + jedis.hget("mm.client.conf", "dupmode") + "<p>" +
							"dupnum  = " + jedis.hget("mm.client.conf", "dupnum") + "<p>" +
							"sockperserver = " + jedis.hget("mm.client.conf", "sockperserver") + "<p>" + 
							"logdupinfo = " + jedis.hget("mm.client.conf", "dupinfo") + "<p>" + "</tt>" +
							"<H2> MMServer DNS: </H2><tt>" +
							PhotoServer.getDNSHtml(conf) + "</tt><p>" +
							"<H1> #Useful Links:</H1><tt>" +
							"<H2><tt><a href=/data>Active Redis Data Sets</a></tt></H2>" +
							"</tt>" +
							"</BODY>" +
							"</HTML>";
			response.getWriter().print(page);
			response.getWriter().flush();
		} catch (JedisException je) {
			response.getWriter().print("Redis ERROR: " + je.getMessage());
			response.getWriter().flush();
		} finally {
			RedisFactory.putInstance(jedis);
		}
	}
	
	private void __gen_json_d(String date, String NAME, long lastTs, 
			int f1, int f2, boolean delta) {
		String fpath = "m/d/";
		ArrayList<String> lines = new ArrayList<String>();
		FileWriter fw = null;

		try {
			File r = new File("log/" + conf.getNodeName() + "." + 
					conf.getServerPort() + "." + date + ".log");
			if (!r.exists()) {
				System.out.println("Can't find the log file for date " + date);
				return;
			}
			FileReader fr = new FileReader(r);
			BufferedReader br = new BufferedReader(fr);
			String line = null, rline;
			Double thisF2 = null, lastF2 = null;
			Long thisF1 = null, lastF1 = null;
			do {
				rline = null;
				line = br.readLine();
				if (line == null) break;
				String[] la = line.split(",");
				if (la != null && la.length > 0) {
					for (int i = 0; i < la.length; i++) {
						if (i == f1) {
							thisF1 = Long.parseLong(la[i]);
							if (rline == null)
								rline = "" + thisF1 * 1000;
							else
								rline += "," + thisF1 * 1000;
							if (lastF1 == null)
								lastF1 = thisF1;
						}
						if (i == f2) {
							if (delta) {
								thisF2 = Double.parseDouble(la[i]);
								if (lastF2 == null) {
									rline = null;
								} else {
									if (thisF2 >= lastF2 && thisF1 > lastF1) {
										if (rline == null)
											rline = "" + (thisF2 - lastF2) / (thisF1 - lastF1);
										else
											rline += "," + (thisF2 - lastF2) / (thisF1 - lastF1);
									} else {
										rline += ",null";
									}
								}
								lastF1 = thisF1;
								lastF2 = thisF2;
							} else {
								if (rline == null)
									rline = la[i];
								else
									rline += "," + la[i];
							}
						}
					}
					if (rline != null) {
						if (thisF1 > lastTs)
							lines.add(rline);
					}
				}
			} while (true);
			br.close();
			File f = new File(fpath + NAME + ".json." + date);
			if (!f.exists()) {
				if (!f.getParentFile().exists())
					f.getParentFile().mkdir();
				f.createNewFile();
			}
			fw = new FileWriter(f, false);
			BufferedWriter w = new BufferedWriter(fw);
			w.write("[\n");
			for (int i = 0; i < lines.size(); i++) {
				String l = lines.get(i);
				w.write("[");
				w.write(l);
				if (i == lines.size() - 1)
					w.write("]\n");
				else
					w.write("],\n");
			}
			w.write("]");
			w.close();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	private void __gen_json_d_rbl(String date, String NAME, long lastTs,
			int f1, int f2, int f3) {
		String fpath = "m/d/";
		ArrayList<String> lines = new ArrayList<String>();
		FileWriter fw = null;

		try {
			File r = new File("log/" + conf.getNodeName() + "." + 
					conf.getServerPort() + "." + date + ".log");
			if (!r.exists()) {
				System.out.println("Can't find the log file for date " + date);
				return;
			}
			FileReader fr = new FileReader(r);
			BufferedReader br = new BufferedReader(fr);
			String line = null, rline;
			Long thisF1 = null;
			Long thisF2 = null, lastF2 = null;
			Long thisF3 = null, lastF3 = null;
			do {
				rline = null;
				line = br.readLine();
				if (line == null) break;
				String[] la = line.split(",");
				if (la != null && la.length > 0) {
					for (int i = 0; i < la.length; i++) {
						if (i == f1) {
							thisF1 = Long.parseLong(la[i]);
							if (rline == null)
								rline = "" + thisF1 * 1000;
							else
								rline += "," + thisF1 * 1000;
						}
						if (i == f2) {
							thisF2 = Long.parseLong(la[i]);
						}
						if (i == f3) {
							thisF3 = Long.parseLong(la[i]);
							if (lastF3 == null) {
								rline += ",null";
							} else {
								if (thisF2 >= lastF2 && thisF3 > lastF3) {
									if (rline == null)
										rline = "" + (thisF2 - lastF2) / (thisF3 - lastF3);
									else
										rline += "," + (thisF2 - lastF2) / (thisF3 - lastF3);
								} else {
									rline += ",null";
								}
							}
							lastF2 = thisF2;
							lastF3 = thisF3;
						}
					}
					if (rline != null) {
						if (thisF1 > lastTs)
							lines.add(rline);
					}
				}
			} while (true);
			br.close();
			File f = new File(fpath + NAME + ".json." + date);
			if (!f.exists()) {
				if (!f.getParentFile().exists())
					f.getParentFile().mkdir();
				f.createNewFile();
			}
			fw = new FileWriter(f, false);
			BufferedWriter w = new BufferedWriter(fw);
			w.write("[\n");
			for (int i = 0; i < lines.size(); i++) {
				String l = lines.get(i);
				w.write("[");
				w.write(l);
				if (i == lines.size() - 1)
					w.write("]\n");
				else
					w.write("],\n");
			}
			w.write("]");
			w.close();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	private void __gen_json_a(String date, String NAME, long lastTs, 
			int f1, int f2) {
		String fpath = "m/a/";
		ArrayList<String> lines = new ArrayList<String>();
		FileWriter fw = null;

		try {
			File r = new File("log/sysinfo-" + date);
			if (!r.exists()) {
				System.out.println("Can't find the sysinfo log file for date " + date);
				return;
			}
			FileReader fr = new FileReader(r);
			BufferedReader br = new BufferedReader(fr);
			String line = null, rline;
			Long thisF1 = null, lastF1 = null;
			do {
				rline = null;
				line = br.readLine();
				if (line == null) break;
				String[] la = line.split(",");
				if (la != null && la.length > 0) {
					for (int i = 0; i < la.length; i++) {
						if (!la[0].endsWith("ALL_MMS"))
							break;
						if (i == f1) {
							thisF1 = Long.parseLong(la[i]);
							if (rline == null)
								rline = "" + thisF1 * 1000;
							else
								rline += "," + thisF1 * 1000;
							if (lastF1 == null)
								lastF1 = thisF1;
						}
						if (i == f2) {
							if (rline == null)
								rline = la[i];
							else
								rline += "," + la[i];
						}
					}
					if (rline != null) {
						if (thisF1 > lastTs)
							lines.add(rline);
					}
				}
			} while (true);
			br.close();
			File f = new File(fpath + NAME + ".json." + date);
			if (!f.exists()) {
				if (!f.getParentFile().exists())
					f.getParentFile().mkdir();
				f.createNewFile();
			}
			fw = new FileWriter(f, false);
			BufferedWriter w = new BufferedWriter(fw);
			w.write("[\n");
			for (int i = 0; i < lines.size(); i++) {
				String l = lines.get(i);
				w.write("[");
				w.write(l);
				if (i == lines.size() - 1)
					w.write("]\n");
				else
					w.write("],\n");
			}
			w.write("]");
			w.close();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	private void __gen_json_a_rbl(String date, String NAME, long lastTs,
			int f1, int f2, int f3) {
		String fpath = "m/a/";
		ArrayList<String> lines = new ArrayList<String>();
		FileWriter fw = null;

		try {
			File r = new File("log/sysinfo-" + date);
			if (!r.exists()) {
				System.out.println("Can't find the sysinfo log file for date " + date);
				return;
			}
			FileReader fr = new FileReader(r);
			BufferedReader br = new BufferedReader(fr);
			String line = null, rline;
			Long thisF1 = null;
			Long thisF2 = null;
			Long thisF3 = null;
			do {
				rline = null;
				line = br.readLine();
				if (line == null) break;
				String[] la = line.split(",");
				if (la != null && la.length > 0) {
					for (int i = 0; i < la.length; i++) {
						if (!la[0].endsWith("ALL_MMS"))
							break;
						if (i == f1) {
							thisF1 = Long.parseLong(la[i]);
							if (rline == null)
								rline = "" + thisF1 * 1000;
							else
								rline += "," + thisF1 * 1000;
						}
						if (i == f2) {
							thisF2 = Long.parseLong(la[i]);
						}
						if (i == f3) {
							thisF3 = Long.parseLong(la[i]);
							if (thisF3 > 0)
								rline += "," + thisF2 / thisF3;
							else
								rline += ",null";
						}
					}
					if (rline != null) {
						if (thisF1 > lastTs)
							lines.add(rline);
					}
				}
			} while (true);
			br.close();
			File f = new File(fpath + NAME + ".json." + date);
			if (!f.exists()) {
				if (!f.getParentFile().exists())
					f.getParentFile().mkdir();
				f.createNewFile();
			}
			fw = new FileWriter(f, false);
			BufferedWriter w = new BufferedWriter(fw);
			w.write("[\n");
			for (int i = 0; i < lines.size(); i++) {
				String l = lines.get(i);
				w.write("[");
				w.write(l);
				if (i == lines.size() - 1)
					w.write("]\n");
				else
					w.write("],\n");
			}
			w.write("]");
			w.close();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	private void __cat_json_file(String fpath, String date, String NAME, 
			long lastTs, PrintWriter pw) {
		try {
			File r = new File(fpath + NAME + ".json." + date);
			if (!r.exists()) 
				return;
			FileReader fr = new FileReader(r);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			do {
				line = br.readLine();
				if (line == null) break;
				pw.write(line);
				pw.write("\n");
			} while (true);
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void doMonitor(String target, Request baseRequest, HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		String date = request.getParameter("date");
		String slastTs = request.getParameter("last");
		long lastTs = 0;

		if (slastTs != null) {
			lastTs = Long.parseLong(slastTs);
		}
		if (target.startsWith("/m/a/")) {
			if (date == null) {
				date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			}
			String fname = target.substring(target.lastIndexOf("/") + 1, 
					target.indexOf("."));
			
			response.getWriter().print(request.getParameter("callback") + "(");
			response.getWriter().flush();
			
			// generate json files in /m/a/ as NAME.yyyy-MM-dd.json
			if (fname.equals("BW")) {
				__gen_json_a(date, "WBW", lastTs, 1, 4);
				__gen_json_a(date, "RBW", lastTs, 1, 5);
				__cat_json_file("m/a/", date, "WBW", lastTs, response.getWriter());
				response.getWriter().print(",");
				__cat_json_file("m/a/", date, "RBW", lastTs, response.getWriter());
				response.getWriter().print(");");
				response.getWriter().flush();
				return;
			} else if (fname.equals("WBW"))
				__gen_json_a(date, "WBW", lastTs, 1, 4);
			else if (fname.equals("RBW"))
				__gen_json_a(date, "RBW", lastTs, 1, 5);
			else if (fname.equals("RLT"))
				__gen_json_a(date, "RLT", lastTs, 1, 6);
			else if (fname.equals("WBS"))
				__gen_json_a(date, "WBS", lastTs, 1, 7);
			else if (fname.equals("RBS"))
				__gen_json_a(date, "RBS", lastTs, 1, 8);
			else if (fname.equals("RDL"))
				__gen_json_a(date, "RDL", lastTs, 1, 9);
			else if (fname.equals("RDN"))
				__gen_json_a(date, "RDN", lastTs, 1, 10);
			else if (fname.equals("RER"))
				__gen_json_a(date, "RER", lastTs, 1, 11);
			else if (fname.equals("WRN"))
				__gen_json_a(date, "WRN", lastTs, 1, 12);
			else if (fname.equals("WER"))
				__gen_json_a(date, "WER", lastTs, 1, 13);
			else if (fname.equals("RBL")) 
				__gen_json_a_rbl(date, "RBL", lastTs, 1, 8, 10);
		}

		if (target.startsWith("/m/d/")) {
			if (date == null) {
				date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			}
			String fname = target.substring(target.lastIndexOf("/") + 1, 
					target.indexOf("."));
			
			response.getWriter().print(request.getParameter("callback") + "(");
			response.getWriter().flush();

			// generate json files in /m/d/ as NAME.yyyy-MM-dd.json
			if (fname.equals("BW")) {
				__gen_json_d(date, "WBW", lastTs, 0, 1, false);
				__gen_json_d(date, "RBW", lastTs, 0, 2, false);
				__cat_json_file("m/d/", date, "WBW", lastTs, response.getWriter());
				response.getWriter().print(",");
				__cat_json_file("m/d/", date, "RBW", lastTs, response.getWriter());
				response.getWriter().print(");");
				response.getWriter().flush();
				return;
			} else if (fname.equals("WBW"))
				__gen_json_d(date, "WBW", lastTs, 0, 1, false);
			else if (fname.equals("RBW"))
				__gen_json_d(date, "RBW", lastTs, 0, 2, false);
			else if (fname.equals("RLT"))
				__gen_json_d(date, "RLT", lastTs, 0, 3, false);
			else if (fname.equals("WBS"))
				__gen_json_d(date, "WBS", lastTs, 0, 4, true);
			else if (fname.equals("RBS"))
				__gen_json_d(date, "RBS", lastTs, 0, 5, true);
			else if (fname.equals("RDL"))
				__gen_json_d(date, "RDL", lastTs, 0, 6, false);
			else if (fname.equals("RDN"))
				__gen_json_d(date, "RDN", lastTs, 0, 7, true);
			else if (fname.equals("RER"))
				__gen_json_d(date, "RER", lastTs, 0, 8, true);
			else if (fname.equals("WRN"))
				__gen_json_d(date, "WRN", lastTs, 0, 9, true);
			else if (fname.equals("WER"))
				__gen_json_d(date, "WER", lastTs, 0, 10, true);
			else if (fname.equals("RBL")) 
				__gen_json_d_rbl(date, "RBL", lastTs, 0, 5, 7);
		}

		ResourceHandler rh = new ResourceHandler();
		rh.setResourceBase(".");
		rh.handle(target, baseRequest, request, response);
		
		response.getWriter().print(");");
		response.getWriter().flush();
	}
	
	private void doData(String target, Request baseRequest, HttpServletRequest request, 
			HttpServletResponse response) throws IOException, ServletException {
		String type = request.getParameter("type");
		String prefix = null;
		
		if (type == null)
			prefix = "";
		else {
			if (type.equalsIgnoreCase("image")) {
				prefix = "i";
			} else if (type.equalsIgnoreCase("text")) {
				prefix = "t";
			} else if (type.equalsIgnoreCase("audio")) {
				prefix = "a";
			} else if (type.equalsIgnoreCase("video")) {
				prefix = "v";
			} else if (type.equalsIgnoreCase("application")) {
				prefix = "o";
			} else if (type.equalsIgnoreCase("thumbnail")) {
				prefix = "s";
			} else if (type.equalsIgnoreCase("other")) {
				prefix = "";
			}
		}
		
		TreeMap<String, SetStats> m = sp.getSetBlks();
		if (m == null) {
			badResponse(baseRequest, response, "#FAIL:read from redis failed.");
			return;
		}
		response.setContentType("text/plain;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		PrintWriter pw = response.getWriter();

		pw.println("# Avaliable type contains: text/image/audio/video/application/thumbnail/other");
		if (prefix == null) 
			return;
		pw.println("# Data Count (Set_Name, Obj_Number, Used_Length(MB), Used_Blocks, SPACE.UR, BLK.UR, SPACE.WASTE(MB):");

		int totallen = 0, totalnr = 0;
		Iterator<String> ir = m.navigableKeySet().descendingIterator();
		while (ir.hasNext()) {
			String set = ir.next();
			if (set.startsWith(prefix)) {
				SetInfo si = ServerHealth.setInfos.get(set);
				totallen += (si == null ? m.get(set).fnr * conf.getBlockSize(): si.totalLength);
				totalnr += m.get(set).rnr;
				pw.println(" " + set + ", " + m.get(set).rnr + ", " +
						(si == null ? 
								((m.get(set).fnr * ((double)conf.getBlockSize() / 1024.0 / 1024.0)) + "^") : 
									String.format("%.4f", si.usedLength / 1024.0 / 1024.0)) + ", " + 
									(si == null ? "-" : si.usedBlocks) + ", " + 
									(si == null ? "-" : String.format("%.2f%%", (si.totalLength == 0 ? 0 : (double)si.usedLength * 100 / si.totalLength))) + ", " +
									(si == null ? "-" : String.format("%.2f%%", (si.totalBlocks == 0 ? 0 : (double)si.usedBlocks * 100 / si.totalBlocks))) + ", " + 
									(si == null ? "-" : ((si.totalLength - si.usedLength) / 1024.0 / 1024.0))
						);
			}
		}
		pw.println(" [TOTAL], " + totalnr + ", " + (String.format("%.4f", (double)totallen / 1024.0 / 1024.0)));
	}
	
	private void doBrowse(String target, Request baseRequest, HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		String set = request.getParameter("set");
		
		if (set == null) {
			response.setContentType("text/html;charset=utf-8");
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			baseRequest.setHandled(true);
			response.getWriter().println("#FAIL: set can not be null");
		} else {
			Set<String> elements = sp.getSetElements(set);
			StringBuilder sb = new StringBuilder();
			sb.append("<HTML> <HEAD> <TITLE> MM Browser </TITLE> </HEAD>" +
					"<BODY><H1> Set = " + set + "</H1><UL>");
			for (String el : elements) {
				sb.append("<li><a href=/get?key=" + set + "@" + el + "><tt>" + el + "</tt></a>");
			}
			sb.append("</UL></BODY> </HTML>");
			
			response.setContentType("text/html;charset=utf-8");
			response.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			response.getWriter().write(sb.toString());
			response.getWriter().flush();
		}
	}
	
	public enum MMType {
		TEXT, VIDEO, AUDIO, IMAGE, THUMBNAIL, APPLICATION, OTHER,
	}
	
	public static String getMMTypeSymbol(MMType type) {
		switch (type) {
		case TEXT:
			return "t";
		case IMAGE:
			return "i";
		case AUDIO:
			return "a";
		case VIDEO:
			return "v";
		case APPLICATION:
			return "o";
		case THUMBNAIL:
			return "s";
		case OTHER:
		default:
			return "";
		}
	}

	private void doDedup(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
		String volume = request.getParameter("vol");
		Map<String, String> di = sp.getDedupInfo();
		if (di == null)
			return;
		TreeMap<String, SetStats> m = sp.getSetBlks();
		if (m == null)
			return;
		int vol = 24, j = 0;
		
		String sdn = sp.getClientConfig("dupnum");
		int idn = sdn == null ? 1 : Integer.parseInt(sdn);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		//get all timestamp
		TreeSet<String> allts = new TreeSet<String>();          

		//get all sets dup num
		HashMap<String, Integer> dupnum = new HashMap<String, Integer>();
		HashMap<String, Integer> tnum = new HashMap<String, Integer>();
		for (Map.Entry<String, String> en : di.entrySet()) {
			String setname = en.getKey().split("@")[0];
			if (Character.isDigit(setname.charAt(0)))
				allts.add(setname);
			else
				allts.add(setname.substring(1));
			Integer n = dupnum.get(setname);
			Integer t = tnum.get(setname);
			int i = n == null ? 0 : n.intValue();
			i += Integer.parseInt(en.getValue());
			dupnum.put(setname, i);
			tnum.put(setname, t == null ? 1 : t + 1);
		}

		StringBuilder page = new StringBuilder("<html> <head> <title>MM Server Dedup Info</title> </head> <body>");
		page.append("<H1> #Server Dedup Info </H1> ");
		page.append("<H3> #Args:{vol=" + vol + "} </H3> ");
		page.append("<h3> 数据含义说明 </h3>");
		page.append("A: 实际存储对象个数 <br>");
		page.append("B: 检测到重复对象个数<br>");
		page.append("C: 检测到重复对象占总写入量的百分比<br>");
		page.append("D: 检测到重复对象（去重后）占实际存储对象个数的百分比<br><p/>");
		page.append("<table border=\"1\" cellpadding=\"4\" cellspacing=\"0\"><tr align=\"center\"> <td>Time</td><td>Set Timestamp</td><td>Text</td><td>Video</td><td>Audio</td><td>Image</td><td>Thumbnail</td><td>Application</td><td>Other</td> </tr>  ");
		Iterator<String> iter = allts.descendingIterator();

		while (iter.hasNext()) {
			if (vol > 0 && ++j > vol)
				break;
			String ts = iter.next();
			Date date = null;
			try {
				date = new Date(Long.parseLong(ts) * 1000);
			} catch (NumberFormatException e){
				System.out.println("Ignore timestamp " + ts);
				--j;
				continue;
			}
			String time = df.format(date);
			SetStats ss = null;
			Integer num = null;
			String key = null;
			int a, b;        
			double c, d;
			
			page.append("<tr align=\"right\"><td>" + time + "</td><td>" + ts + "</td>");
			
			for (MMType type : MMType.values()) {
				key = getMMTypeSymbol(type) + ts;
				ss = m.get(key);
				num = dupnum.get(key);
				a = (int) (ss == null ? 0 : ss.rnr);
				b = (num == null ? 0 : num.intValue()) / idn;
				c = a + b == 0 ? 0 : b / (double)(a + b);
				if (a != 0 && tnum.get(key) != null) d = tnum.get(key) / (double)a; else d = 0.0;
				page.append("<td>" + a + "<br>"+ b + "<br>" + (String.format("%.2f%%", c * 100)) + "<br>" + (String.format("%.2f%%", d * 100)) + "</td>");
			}
		}
		page.append("</table></body> </html>");

		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		try {
			response.getWriter().write(page.toString());
			response.getWriter().flush();
		} catch (IOException e) {
			e.printStackTrace();
			try {
				badResponse(baseRequest, response, e.getMessage());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	private void doDailydup(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
		String volume = request.getParameter("vol");
		int vol = 7, j = 0;
		
		Map<String, String> di = sp.getDedupInfo();
		if (di == null)
			return;

		TreeMap<String, SetStats> m = sp.getSetBlks();
		if (m == null)
			return;

		if (volume != null) {
			if (volume.equalsIgnoreCase("all")) {
				vol = -1;
			} else {
				try {
					vol = Integer.parseInt(volume);
				} catch (Exception e) {
				}
			}
		}
		String sdn = sp.getClientConfig("dupnum");
		int idn = sdn == null ? 1 : Integer.parseInt(sdn);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		//映射关系time -> type -> 
		//一天存入系统的对象的数量，经过md5去重
		TreeMap<String, HashMap<String, Set<String>>> dayreal = new TreeMap<String, HashMap<String, Set<String>>>();
		//一天存入系统的对象的数量，不去重，把每个小时的对象数量相加
		HashMap<String, HashMap<String, Integer>> daydup = new HashMap<String, HashMap<String, Integer>>();
		//一天的重复数，mm.dedup.info的value相加的结果
		HashMap<String, HashMap<String, Integer>> dayreal2 = new HashMap<String, HashMap<String, Integer>>();
		//一天里发生重复的对象的个数
		HashMap<String, HashMap<String, Set<String>>> daydup2 = new HashMap<String, HashMap<String, Set<String>>>();
		//get all timestamp
		TreeSet<String> allts = new TreeSet<String>();          
		for (String setname : m.keySet()) {
			if (Character.isDigit(setname.charAt(0)))
				allts.add(setname);
			else
				allts.add(setname.substring(1));
		}

		//get all sets dup num
		HashMap<String, Integer> dupnum = new HashMap<String, Integer>();
		for (Map.Entry<String, String> en : di.entrySet()) {
			String[] setmd5 = en.getKey().split("@");
			String setname = setmd5[0];
			String md5 = setmd5[1];
			Integer n = dupnum.get(setname);
			int i = n == null ? 0 : n.intValue();
			i += Integer.parseInt(en.getValue());
			dupnum.put(setname, i);

			Date date = null;
			long ts;
			String type;
			try {
				if (Character.isDigit(setname.charAt(0))) {
					type = "";
					ts = Long.parseLong(setname);
				} else {
					type = setname.charAt(0) + "";
					ts = Long.parseLong(setname.substring(1));
				}
				date = new Date(ts * 1000);
			} catch(NumberFormatException e) {
				System.out.println("Ignore timestamp " + setname);
				continue;
			}
			String time = df.format(date);
			if (!daydup2.containsKey(time))
				daydup2.put(time, new HashMap<String, Set<String>>());
			Set<String> md5s = daydup2.get(time).get(type); 
			if (md5s == null)
				md5s = new HashSet<String>();
			md5s.add(md5);
			daydup2.get(time).put(type, md5s);
		}

		String lastTs = null;
		Iterator<String> iter = allts.descendingIterator();
		while (iter.hasNext()) {
			String ts = iter.next();
			Date date = null;
			try {
				date = new Date(Long.parseLong(ts) * 1000);
			} catch (NumberFormatException e){
				System.out.println("Ignore timestamp " + ts);
				continue;
			}
			lastTs = df2.format(date);
			String time = df.format(date);
			if (dayreal.size() == vol && !dayreal.containsKey(time))
				break;
			if (!dayreal.containsKey(time))
				dayreal.put(time, new HashMap<String, Set<String>>());
			if (!dayreal2.containsKey(time))
				dayreal2.put(time, new HashMap<String, Integer>());
			if (!daydup.containsKey(time))
				daydup.put(time, new HashMap<String, Integer>());
			Integer num = null;
			String key = null;

			for (MMType type : MMType.values()) {
				key = getMMTypeSymbol(type) + ts;
				Set<String> md5s = dayreal.get(time).get(getMMTypeSymbol(type));
				if (md5s == null)
					md5s = new HashSet<String>();
				md5s.addAll(sp.getSetElements(key));

				dayreal.get(time).put(getMMTypeSymbol(type), md5s);

				Integer in = daydup.get(time).get(getMMTypeSymbol(type));
				if (in == null)
					in = 0;
				num = dupnum.get(key);
				in += (num == null ? 0 : num.intValue()) / idn;
				daydup.get(time).put(getMMTypeSymbol(type), in);

				Integer in2 = dayreal2.get(time).get(getMMTypeSymbol(type));
				if (in2 == null)
					in2 = 0;
				SetStats ss = m.get(key);
				int a = (int) (ss == null ? 0 : ss.rnr);
				in2 += a;
				dayreal2.get(time).put(getMMTypeSymbol(type), in2);
			}
		}
		
		StringBuilder page = new StringBuilder("<html> <head> <title>MM Server Daily Dup Info</title> </head> <body>");
		page.append("<H1> #Server Daily Dup Info </H1> ");
		page.append("<H3> #Args:{vol=" + vol + "} </H3> ");
		page.append("<h3> 数据含义说明 </h3>");
		page.append("A: 真正对象个数（跨集合去重） <br>");
		page.append("B: 实际存储象个数（多集合合并）<br>");
		page.append("C: 检测到的重复的对象个数（跨集合去重） <br>");
		page.append("D: 检测到的对象的重复次数 <br>");
		page.append("E: 检测到的重复的对象个数占真正对象个数的百分比<br>");
		page.append("F: 检测到的重复占总访问量的百分比<br><p/>");
		page.append("<table border=\"1\" cellpadding=\"4\" cellspacing=\"0\"><tr align=\"center\"> <td>Time</td><td>Text</td><td>Video</td><td>Audio</td><td>Image</td><td>Thumbnail</td><td>Application</td><td>Other</td> </tr>  ");
		int a = 0, b = 0 ,d, f;        
		double c, g;
		
		for (String time : dayreal.descendingKeySet()) {
			page.append("<tr align=\"right\"><td>" + time + "</td>");
			for (MMType type : MMType.values()) {
				a = dayreal.get(time).get(getMMTypeSymbol(type)).size();
				b = daydup.get(time).get(getMMTypeSymbol(type));
				if (daydup2.get(time) != null) {
					Set<String> s1 = daydup2.get(time).get(getMMTypeSymbol(type));
					f = s1 == null ? 0 :s1.size();
				} else 
					f = 0;
				d = dayreal2.get(time).get(getMMTypeSymbol(type));
				c = a == 0 ? 0 : f/(double)a;
				g = (b + d) == 0 ? 0 : ((double)b / (d + b));
				page.append("<td>" + a + "<br>" + d +"<br> "+ f + "<br> "+ b + "<br>" + 
						(String.format("%.2f%%", c * 100)) + "<br>" + 
						(String.format("%.2f%%", 100 * g)) + "</td>");
			}
			page.append("</tr>");
		}
		page.append("</table>");
		page.append("<p>Last Checked Set Timestamp is : <font color=\"red\">" + lastTs + "</font> (not included)");
		page.append("</p></body></html>");

		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		try {
			response.getWriter().write(page.toString());
			response.getWriter().flush();
		} catch (IOException e) {
			e.printStackTrace();
			try {
				badResponse(baseRequest, response, e.getMessage());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	private void doP2p(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
		Map<String, String> di = sp.getDedupInfo();
		TreeSet<String> tset = new TreeSet<String>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String volume = request.getParameter("vol");
		String sk = request.getParameter("k");
		int k = 5, vol = 24, j = 0;
		
		if (sk != null) {
			try {
				k = Integer.parseInt(sk);
			} catch (NumberFormatException nfe) {
			}
		}
		if (volume != null) {
			if (volume.equalsIgnoreCase("all")) {
				vol = -1;
			} else {
				try {
					vol = Integer.parseInt(volume);
				} catch (Exception e) {
				}
			}
		}
		TreeMap<String, Set<String>> allsets = new TreeMap<String, Set<String>>();
		for (Map.Entry<String, String> en : di.entrySet()) {
			String[] keys = en.getKey().split("@");
			if (keys.length == 2) {
				Set<String> elements = allsets.get(keys[0]);
				if (elements == null)
					elements = new TreeSet<String>();
				elements.add(keys[1]);
				allsets.put(keys[0], elements);
				if (Character.isDigit(keys[0].charAt(0)))
					tset.add(keys[0]);
				else
					tset.add(keys[0].substring(1));
			}
		}
		
		StringBuilder page = new StringBuilder("<html> <head> <title>MM Server P2P Info</title> </head> <body>");
		page.append("<H1> #Server P2P CX Info </H1> ");
		page.append("<H3> #Args:{vol=" + vol + ",k=" + k + "} </H3> ");
		page.append("<table border=\"1\" cellpadding=\"4\" cellspacing=\"0\"><tr align=\"center\"> <td>Time</td><td>Set Timestamp</td><td>Text</td><td>Video</td><td>Audio</td><td>Image</td><td>Thumbnail</td><td>Application</td><td>Other</td> </tr>  ");
		Random rand = new Random();

		for (String ts : tset.descendingSet()) {
			Date date = null;
			
			if (vol > 0 && ++j > vol)
				break;
			try {
				date = new Date(Long.parseLong(ts) * 1000);
			} catch (NumberFormatException e) {
				System.out.println("Ignore timestamp " + ts);
				--j;
				continue;
			}
			String time = df.format(date);
			
			page.append("<tr><td>" + time + "</td><td>" + ts + "</td>");
			
			for (MMType type : MMType.values()) {
				String setname = getMMTypeSymbol(type) + ts;
				Set<String> all = sp.getSetElements(setname);
				int nr = 0;
				
				if (all != null && allsets.get(setname) != null) {
					all.removeAll(allsets.get(setname));
				}
				if (all != null)
					nr = all.size();
				
				page.append("<td>" + nr + "<br>");
				if (all != null && nr > 0) {
					String[] allArray = all.toArray(new String[0]);
					int alen = Math.min(k, allArray.length);
					Set<Integer> idx = new TreeSet<Integer>();
					do {
						int n = rand.nextInt();
						if (n < 0)
							n = -n;
						idx.add(n % allArray.length);
					} while (idx.size() < alen);
					Integer[] idxArray = idx.toArray(new Integer[0]);
					for (int i = 0; i < alen; i++) {
						if (type == MMType.THUMBNAIL)
							page.append("<img src=/get?key=" + setname + "@" + allArray[idxArray[i]] + ">");
						else if (type == MMType.IMAGE)
							page.append("<img width=\"100\" height=\"100\" src=/get?key=" + setname + "@" + allArray[idxArray[i]] + ">");
					}
				}
				page.append("</td>");
			}
			page.append("</tr>");
		}
		page.append("</table></body> </html>");
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		try {
			response.getWriter().write(page.toString());
			response.getWriter().flush();
		} catch (IOException e) {
			e.printStackTrace();
			try {
				badResponse(baseRequest, response, e.getMessage());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	private void doTopdup(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
		Map<String, String> di = sp.getDedupInfo();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		TreeMap<String, HashMap<String, TopKeySet>> topd = new TreeMap<String, HashMap<String, TopKeySet>>();
		String sdn = sp.getClientConfig("dupnum");
		int idn = sdn == null ? 1 : Integer.parseInt(sdn);
		String volume = request.getParameter("vol");
		String sk = request.getParameter("k");
		int k = 5, vol = 24, j = 0;
		
		if (sk != null) {
			try {
				k = Integer.parseInt(sk);
			} catch (NumberFormatException nfe) {
			}
		}
		if (volume != null) {
			if (volume.equalsIgnoreCase("all")) {
				vol = -1;
			} else {
				try {
					vol = Integer.parseInt(volume);
				} catch (Exception e) {
				}
			}
		}
		for (Map.Entry<String, String> en : di.entrySet()) {
			String setname = en.getKey().split("@")[0];
			HashMap<String,TopKeySet> h1;
			TopKeySet h2;
			String type;
			switch(setname.charAt(0)) {
			case 't':
				type = "text";
				setname = setname.substring(1);
				break;
			case 'i':
				type = "image";
				setname = setname.substring(1);
				break;
			case 'a':
				type = "audio";
				setname = setname.substring(1);
				break;
			case 'v':
				type = "video";
				setname = setname.substring(1);
				break;
			case 'o':
				type = "application";
				setname = setname.substring(1);
				break;
			case 's':
				type = "thumbnail";
				setname = setname.substring(1);
				break;
			default:
				type = "other";
				break;
			}
			h1 = topd.get(setname);
			if (h1 == null) {
				h1 = new HashMap<String,TopKeySet>();
				topd.put(setname, h1);
			}
			h2 = h1.get(type);
			if (h2 == null) {
				h2 = new TopKeySet(k);
				h1.put(type, h2);
			}
			h2.put(en.getKey(), Long.parseLong(en.getValue()) / idn);
		}

		StringBuilder page = new StringBuilder("<html> <head> <title>MM Server Top Dup</title> </head> <body>");
		page.append("<H1> #Server Top Dup </H1> ");
		page.append("<H3> #Args:{vol=" + vol + ",k=" + k + "} </H3> ");
		page.append("<table rules=\"all\" border=\"1\" cellpadding=\"4\" cellspacing=\"0\"><tr align=\"center\"> <td>Time</td><td>Set Timestamp</td><td>Text</td><td>Video</td><td>Audio</td><td>Image</td><td>Thumbnail</td><td>Application</td><td>Other</td></tr>");

		Iterator<String> iter = topd.descendingKeySet().iterator();

		while (iter.hasNext()) {
			String ts = iter.next();
			Date date = null;
			
			if (vol > 0 && ++j > vol)
				break;
			try {
				date = new Date(Long.parseLong(ts) * 1000);
			} catch(NumberFormatException e){
				System.out.println("Ignore timestamp " + ts);
				--j;
				continue;
			}
			String time = df.format(date);
			page.append("<tr><td>" + time + "</td><td>" + ts + "</td>");
			for (String type : new String[]{"text","video","audio","image","thumbnail","application","other"}) {
				page.append("<td>");
				if (topd.get(ts) != null && topd.get(ts).get(type) != null) {
					int idx = 0;
					for (KeySetEntry en : topd.get(ts).get(type).ll) {
						idx++;
						if (type.equalsIgnoreCase("thumbnail")) {
							page.append("<img src=/get?key=" + en.key + "> " + en.dn + "<br>");
						} else
							page.append("<a href=/get?key=" + en.key + ">C" + idx + "</a> " + en.dn + "<br>");
					}
				}
				page.append("</td>");
			}
			page.append("</tr>");
		}
		page.append("</table></body> </html>");
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		try {
			response.getWriter().write(page.toString());
			response.getWriter().flush();
		} catch (IOException e) {
			e.printStackTrace();
			try {
				badResponse(baseRequest, response, e.getMessage());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	class TopKeySet {
		public LinkedList<KeySetEntry> ll;
		private int k;
		
		public TopKeySet(int k) {
			ll = new LinkedList<KeySetEntry>();
			this.k = k;
		}
		
		class KeySetEntry {
			String key;
			Long dn;
			
			public KeySetEntry(String key, Long dn) {
				this.key = key;
				this.dn = dn;
			}
		}

		public void put(String key, Long value) {
			boolean isInserted = false;
			for (int i = 0; i < ll.size(); i++) {
				if (value.longValue() > ll.get(i).dn) {
					ll.add(i, new KeySetEntry(key,value));
					isInserted = true;
					break;
				}
			}
			if (ll.size() < k) {
				if (!isInserted)
					ll.addLast(new KeySetEntry(key,value));
			} else if (ll.size() > k) {
				ll.removeLast();
			}
		}
	}

	public void handle(String target, Request baseRequest, HttpServletRequest request, 
			HttpServletResponse response) throws IOException, ServletException {
//		System.out.println(target);
		if (target == null) {
			// bad response
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		} else if (target.equalsIgnoreCase("/get")) {
			doGet(target, baseRequest, request, response);
		} else if (target.equalsIgnoreCase("/put")) {
			doPut(target, baseRequest, request, response);
		} else if (target.equalsIgnoreCase("/info")) {
			doInfo(target, baseRequest, request, response);
		} else if (target.equalsIgnoreCase("/data")) {
			doData(target, baseRequest, request, response);
		} else if (target.equalsIgnoreCase("/b")) {
			doBrowse(target, baseRequest, request, response);
		} else if (target.startsWith("/im/")) {
			badResponse(baseRequest, response, "#FAIL: Please use MM Object Searcher instead.");
		} else if (target.startsWith("/dedup")){
			doDedup(target, baseRequest, request, response);
		} else if (target.startsWith("/topdup")){
			doTopdup(target, baseRequest, request, response);
		} else if (target.startsWith("/dailydup")) {
			doDailydup(target, baseRequest, request, response);
		} else if (target.startsWith("/m/")) {
			doMonitor(target, baseRequest, request, response);
		} else if (target.startsWith("/p2p")) {
			try {
				doP2p(target, baseRequest, request, response);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		}
	}
	     
}
