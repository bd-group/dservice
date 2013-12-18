package iie.mm.server;

import iie.mm.server.StorePhoto.RedirectException;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

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
		// FIXME: text/image/audio/video/application
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
		response.setContentType("text/plain;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getWriter().println("#In Current Server Session:");
		response.getWriter().println(" Total Written Bytes (B): " + ServerProfile.writtenBytes.longValue());
		response.getWriter().println(" Total Read    Bytes (B): " + ServerProfile.readBytes.longValue());
		response.getWriter().println(" Avg Read Latency   (ms): " + (double)ServerProfile.readDelay.longValue() / ServerProfile.readN.longValue());
		response.getWriter().println(PhotoServer.getServerInfo(conf));
		response.getWriter().flush();
	}
	private void doData(String target, Request baseRequest, HttpServletRequest request, 
			HttpServletResponse response) throws IOException, ServletException {
		Map<String,Integer> m = sp.getSetBlks();
		if(m == null)
		{
			badResponse(baseRequest, response, "#FAIL:read from redis failed.");
			return;
		}
		response.setContentType("text/plain;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		PrintWriter pw = response.getWriter();
		pw.println("#Data Count(M) :");
		int total = 0;
		for(Map.Entry<String, Integer> en : m.entrySet())
		{
			total += en.getValue();
			pw.println(en.getKey() + " : " + (en.getValue() * ((double)conf.getBlockSize()/1024.0/1024.0)));
		}
		pw.println("total : "+total * ((double)conf.getBlockSize()/1024.0/1024.0));
		
		
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
		} else {
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		}
	}
	     
}
