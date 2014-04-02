package iie.mm.server;

import iie.mm.server.StorePhoto.RedirectException;
import iie.mm.server.StorePhoto.SetStats;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import javax.servlet.Servlet;
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
		response.setContentType("image");
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
						okResponse(baseRequest, response, content);
					} 
				} catch (RedirectException e) {
					redirectResponse(baseRequest, response, e);
				}
			} else {
				badResponse(baseRequest, response, "#FAIL: invalid key format {" + key + "}");
			}
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
							"sockperserver = " + jedis.hget("mm.client.conf", "sockperserver") + "<p>" + "</tt>" +
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
	
	private void doImageMatch(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		if (target.equalsIgnoreCase("/im/index.html")) {
			ResourceHandler rh = new ResourceHandler();
			rh.setResourceBase(".");
			rh.handle(target, baseRequest, request, response);
		} else {
			DiskFileItemFactory factory = new DiskFileItemFactory();
			ServletFileUpload upload = new ServletFileUpload(factory);
			List<FileItem> items;
			try {
				items = upload.parseRequest(request);
				// 解析request请求
				Iterator<FileItem> iter = items.iterator();
				int distance = 0;
				BufferedImage img = null;
				while (iter.hasNext()) {
					FileItem item = (FileItem) iter.next();
					if (item.isFormField()) { 				// 如果是表单域 ，就是非文件上传元素
						String name = item.getFieldName(); // 获取name属性的值
						String value = item.getString(); // 获取value属性的值
						// System.out.println(name+"   "+value);
						if (name.equals("distance"))
							distance = Integer.parseInt(value);
					} else {
						String fieldName = item.getFieldName(); // 文件域中name属性的值
						String fileName = item.getName(); // 文件的全路径，绝对路径名加文件名
						// okResponse(baseRequest, response, item.get());
						try {
							img = ImageMatch.readImage(item.get());
						} catch (IOException e) {
							badResponse(baseRequest, response,"#FAIL:" + e.getMessage());
							e.printStackTrace();
							return;
						}
						if (img == null) {
							badResponse(baseRequest, response,"#FAIL:the file uploaded probably is not an image.");
							return;
						}

					}
				}
				// String hc = ImageMatch.getPHashcode(img);
				String hc = new ImagePHash().getHash(img);
				TreeMap<String, String> dk = sp.imageMatch(hc, distance);
				String page = "<HTML> <HEAD> <TITLE> MM Browser </TITLE> </HEAD>"
						+ "<BODY><H1> match result </H1><UL>";
				Iterator<String> iter2 = dk.navigableKeySet().iterator();
				while (iter2.hasNext()) {
					String dis = iter2.next();
					page += "<li>" + dk.get(dis) + "<br><img src=\"http://"
							+ request.getLocalAddr() + ":"
							+ request.getLocalPort() + "/get?key="
							+ dk.get(dis) + "\"> </li>";
				}
				page += "</UL></BODY> </HTML>";
				response.setContentType("text/html;charset=utf-8");
				response.setStatus(HttpServletResponse.SC_OK);
				baseRequest.setHandled(true);
				response.getWriter().write(page);
				response.getWriter().flush();
			} catch (FileUploadException e) {
				e.printStackTrace();
				badResponse(baseRequest, response, e.getMessage());
			}
		}

	}

	public void handle(String target, Request baseRequest, HttpServletRequest request, 
			HttpServletResponse response) throws IOException, ServletException {
//		System.out.println(target);
//		System.out.println(request.getRequestURL().toString());
//		System.out.println(request.getLocalAddr());
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
		} else if (target.startsWith("/im/")){
			doImageMatch(target, baseRequest, request, response);
		} else {
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		}
	}
	     
}
