package iie.mm.server;

import iie.mm.server.HTTPHandler.TopKeySet.KeySetEntry;
import iie.mm.server.StorePhoto.RedirectException;
import iie.mm.server.StorePhoto.SetStats;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import redis.clients.jedis.Jedis;

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
				byte[] content = null;
				try {
					content = sp.getPhoto(infos[0], infos[1]);
					if (content == null || content.length == 0) {
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
						okResponse(baseRequest, response, content);
					}
				} catch (RedirectException e) {
					redirectResponse(baseRequest, response, e);
				}
			} else if (infos.length == 7) {
				byte[] content = null;
				try {
					content = sp.searchPhoto(key, null);
					if (content == null || content.length == 0) {
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
						okResponse(baseRequest, response, content);
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
							"Total Written Bytes (B): " + ServerProfile.writtenBytes.longValue() + "<p>" +
							"Total Read    Bytes (B): " + ServerProfile.readBytes.longValue() + "<p>" +
							"Avg Read Latency   (ms): " + (double)ServerProfile.readDelay.longValue() / ServerProfile.readN.longValue() + "<p></tt>" +
							PhotoServer.getServerInfoHtml(conf) + "<p>" +
							PhotoServer.getSpaceInfoHtml(conf) + "<p>" + 
							"<H1> #Client Auto Config: </H1><tt>" +
							"dupmode = " + jedis.hget("mm.client.conf", "dupmode") + "<p>" +
							"dupnum  = " + jedis.hget("mm.client.conf", "dupnum") + "<p>" +
							"sockperserver = " + jedis.hget("mm.client.conf", "sockperserver") + "<p>" + 
							"logdupinfo = " + jedis.hget("mm.client.conf", "dupinfo") + "<p>" + "</tt>" +
							"<H1> #Useful Links:</H1><tt>" +
							"<H2><tt><a href=/data>Active Data Sets</a></tt></H2>" +
							"</tt>" +
							"</BODY>" +
							"</HTML>";
			response.getWriter().print(page);
			response.getWriter().flush();
		} finally {
			RedisFactory.putInstance(jedis);
		}
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
		pw.println("# Data Count (Set_Name, Number, Length(MB)):");

		int totallen = 0, totalnr = 0;
		Iterator<String> ir = m.navigableKeySet().descendingIterator();
		while (ir.hasNext()) {
			String set = ir.next();
			if (set.startsWith(prefix)) {
				totallen += m.get(set).fnr;
				totalnr += m.get(set).rnr;
				pw.println(" " + set + ", " + m.get(set).rnr + ", " + (m.get(set).fnr * ((double)conf.getBlockSize() / 1024.0 / 1024.0)));
			}
		}
		pw.println(" [TOTAL], " + totalnr + ", " + totallen * ((double)conf.getBlockSize() / 1024.0 / 1024.0));
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
		Map<String, String> di = sp.getDedupInfo();
		if (di == null)
			return;
		
		TreeMap<String, SetStats> m = sp.getSetBlks();
		if (m == null)
			return;
		
		String sdn = sp.getClientConfig("dupnum");
		int idn = sdn==null ? 1 : Integer.parseInt(sdn);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
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
		HashMap<String, Integer> tnum = new HashMap<String, Integer>();
		for (Map.Entry<String, String> en : di.entrySet()) {
			String setname = en.getKey().split("@")[0];
			Integer n = dupnum.get(setname);
			Integer t = tnum.get(setname);
			int i = n == null ? 0 : n.intValue();
			i += Integer.parseInt(en.getValue());
			dupnum.put(setname, i);
			tnum.put(setname, t == null ? 1 : t + 1);
		}

		StringBuilder page = new StringBuilder("<html> <head> <title>MM Server Dedup Info</title> </head> <body>");
		page.append("<H1> #Server Dedup Info </H1> ");
		page.append("<table border=\"1\" cellpadding=\"4\" cellspacing=\"0\"><tr align=\"center\"> <td>Time</td><td>Set Timestamp</td><td>Text</td><td>Video</td><td>Audio</td><td>Image</td><td>Thumbnail</td><td>Application</td><td>Other</td> </tr>  ");
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
				page.append("<td>" + a + " <br> "+ b + "<br>" + (String.format("%.2f%%", c * 100)) + "<br>" + (String.format("%.2f%%", d * 100)) + "</td>");
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
	
	private void doP2p(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) {
		Map<String, String> di = sp.getDedupInfo();
		TreeSet<String> tset = new TreeSet<String>();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String sk = request.getParameter("k");
		int k = 5;
		
		if (sk != null) {
			try {
				k = Integer.parseInt(sk);
			} catch (NumberFormatException nfe) {
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
				if (Character.isDigit(keys[0].charAt(0)))
					tset.add(keys[0]);
				else
					tset.add(keys[0].substring(1));
			}
		}
		
		StringBuilder page = new StringBuilder("<html> <head> <title>MM Server P2P Info</title> </head> <body>");
		page.append("<H1> #Server P2P CX Info </H1> ");
		page.append("<table border=\"1\" cellpadding=\"4\" cellspacing=\"0\"><tr align=\"center\"> <td>Time</td><td>Set Timestamp</td><td>Text</td><td>Video</td><td>Audio</td><td>Image</td><td>Thumbnail</td><td>Application</td><td>Other</td> </tr>  ");
		Random rand = new Random();

		for (String ts : tset.descendingSet()) {
			Date date = null;
			
			try {
				date = new Date(Long.parseLong(ts) * 1000);
			} catch (NumberFormatException e) {
				System.out.println("Ignore timestamp " + ts);
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
		String sk = request.getParameter("k");
		int k = 5;
		
		if (sk != null) {
			try {
				k = Integer.parseInt(sk);
			} catch (NumberFormatException nfe) {
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
		page.append("<table rules=\"all\" border=\"1\" cellpadding=\"4\" cellspacing=\"0\"><tr align=\"center\"> <td>Time</td><td>Set Timestamp</td><td>Text</td><td>Video</td><td>Audio</td><td>Image</td><td>Thumbnail</td><td>Application</td><td>Other</td></tr>");

		Iterator<String> iter = topd.descendingKeySet().iterator();

		while (iter.hasNext()) {
			String ts = iter.next();
			Date date = null;
			try {
				date = new Date(Long.parseLong(ts) * 1000);
			} catch(NumberFormatException e){
				System.out.println("Ignore timestamp " + ts);
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
		} else if (target.startsWith("/dedup")){
			doDedup(target, baseRequest, request, response);
		} else if (target.startsWith("/topdup")){
			doTopdup(target, baseRequest, request, response);
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
