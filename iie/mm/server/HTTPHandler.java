package iie.mm.server;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;

import java.io.IOException;

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
	}
	
	private void notFoundResponse(Request baseRequest, HttpServletResponse response, String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_NOT_FOUND);
		baseRequest.setHandled(true);
		response.getWriter().println(message);
	}
	
	private void okResponse(Request baseRequest, HttpServletResponse response, byte[] content) throws IOException {
		// FIXME: text/image/audio/video/application
		response.setContentType("image");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getOutputStream().write(content);
		response.getOutputStream().flush();
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
			String[] infos = key.split("@");
			
			if (infos.length == 2) {
				byte[] content = sp.getPhoto(infos[0], infos[1]);
				if (content == null || content.length == 0) {
					notFoundResponse(baseRequest, response, "#FAIL:can not find any MM object by key=" + key);
				} else {
					okResponse(baseRequest, response, content);
				}
			} else if (infos.length == 7) {
				byte[] content = sp.searchPhoto(key);
				if (content == null || content.length == 0) {
					notFoundResponse(baseRequest, response, "#FAIL:can not find any MM object by key=" + key);
				} else {
					okResponse(baseRequest, response, content);
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
		} else {
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		}
	}
	     
}