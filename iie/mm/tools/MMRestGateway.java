package iie.mm.tools;

import iie.mm.client.ClientAPI;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class MMRestGateway {

	public static class RGRequestWrapper extends HttpServletRequestWrapper {
		private final byte[] body;
		
		public RGRequestWrapper(HttpServletRequest request) throws IOException {
			super(request);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				InputStream inputStream = request.getInputStream();
				if (inputStream != null) {
					byte[] b = new byte[4096];
					int bytesRead = -1;
					while ((bytesRead = inputStream.read(b)) > 0) {
						baos.write(b, 0, bytesRead);
					}
				}
			} catch (IOException ex) {
				throw ex;
			} finally {
				baos.close();
			}
			body = baos.toByteArray();
		}

		@Override
		public ServletInputStream getInputStream() throws IOException {
			final ByteArrayInputStream byteArrayInputStream = 
					new ByteArrayInputStream(body);
			ServletInputStream servletInputStream = new ServletInputStream() {
				public int read() throws IOException {
					return byteArrayInputStream.read();
				}
			};
			return servletInputStream;
		}

		@Override
		public BufferedReader getReader() throws IOException {
			return new BufferedReader(new InputStreamReader(this.getInputStream()));
		}

		public byte[] getBody() {
			return this.body;
		}
	}
	
	public static class RestGatewayHandler extends AbstractHandler {
		private ClientAPI ca;
		
		public RestGatewayHandler(ClientAPI ca) {
			this.ca = ca;
		}

		private void badResponse(Request baseRequest, HttpServletResponse response, 
				String message) throws IOException {
			response.setContentType("text/html;charset=utf-8");
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			baseRequest.setHandled(true);
			response.getWriter().println(message);
			response.getWriter().flush();
		}

		private void okResponse(Request baseRequest, HttpServletResponse response, 
				byte[] content) throws IOException {
			// FIXME: text/image/audio/video/application/thumbnail/other
			response.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			response.getOutputStream().write(content);
			response.getOutputStream().flush();
		}

		private void doOp(String target, Request baseRequest, 
				HttpServletRequest request, HttpServletResponse response) throws IOException {
			RGRequestWrapper rgRequest = 
					new RGRequestWrapper((HttpServletRequest) request);
			String message = null;

			System.out.println("M=" + rgRequest.getMethod() + ", PKey=" + 
					rgRequest.getParameter("key"));

			if (rgRequest.getMethod().equalsIgnoreCase("GET")) {
				String key = rgRequest.getParameter("key");

				if (key == null) {
					badResponse(baseRequest, response, "#FAIL: invalid key '" + 
							key + "'");
					return;
				}
				// do GET the key
				try {
					byte[] b = ca.get(key);
					if (b != null)
						okResponse(baseRequest, response, 
								Arrays.copyOfRange(b, 0, b.length));
					else
						badResponse(baseRequest, response, 
								"#FAIL: null reply from MMServer for key=" + key);
				} catch (Exception e) {
					message = "#FAIL: [GET] key=" + key + " exception: " + e.getMessage();
					System.out.println(message);
					badResponse(baseRequest, response, message);
					return;
				}
			} else if (rgRequest.getMethod().equalsIgnoreCase("POST") ||
					rgRequest.getMethod().equalsIgnoreCase("PUT")) {
				String key = rgRequest.getParameter("key");

				if (key == null) {
					badResponse(baseRequest, response, "#FAIL: invalid key '" + 
							key + "'");
					return;
				}

				// do POST/PUT the key
				byte[] body = rgRequest.getBody();
				if (body.length != rgRequest.getContentLength()) {
					System.out.println("-> GOT DATA LENGTH: " + body.length + 
							", Content LENGTH: " + rgRequest.getContentLength());
				}
				try {
					String info = ca.put(key, body);
					System.out.println("-> Key=" + key + " info=" + info);
					okResponse(baseRequest, response, info.getBytes());
				} catch (Exception e) {
					message = "#FAIL: [PUT] key=" + key + " exception: " + e.getMessage();
					System.out.println(message);
					badResponse(baseRequest, response, message);
					return;
				}
			} else if (rgRequest.getMethod().equalsIgnoreCase("DELETE")) {
				String key = rgRequest.getParameter("key");
				String set = rgRequest.getParameter("set");
				
				if (key == null && set == null) {
					badResponse(baseRequest, response, "#FAIL: invalid key '" + 
							key + "'");
					return;
				}
				// do DELETE key or set
				badResponse(baseRequest, response, "#FAIL: need to upgrade your MM service");
			} else {
				badResponse(baseRequest, response, "#FAIL: unknown OP method: " + 
						rgRequest.getMethod());
			}
		}

		@Override
		public void handle(String target, Request baseRequest, HttpServletRequest request,
				HttpServletResponse response) throws IOException, ServletException {
			try {
				if (target == null) {
					badResponse(baseRequest, response, "#FAIL: invalid target=" + 
							target);
				} else if (target.equalsIgnoreCase("/r/op")) {
					doOp(target, baseRequest, request, response);
				} else {
					badResponse(baseRequest, response, "#FAIL: invalid target=" + 
							target);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<String> argsList = new ArrayList<String>();  
	    List<Option> optsList = new ArrayList<Option>();
	    List<String> doubleOptsList = new ArrayList<String>();
	    
	    // parse the args
		for (int i = 0; i < args.length; i++) {
			System.out.println("Args " + i + ", " + args[i]);
			switch (args[i].charAt(0)) {
	        case '-':
	            if (args[i].length() < 2)
	                throw new IllegalArgumentException("Not a valid argument: "+args[i]);
	            if (args[i].charAt(1) == '-') {
	                if (args[i].length() < 3)
	                    throw new IllegalArgumentException("Not a valid argument: "+args[i]);
	                doubleOptsList.add(args[i].substring(2, args[i].length()));
	            } else {
	                if (args.length-1 > i)
	                    if (args[i + 1].charAt(0) == '-') {
	                    	optsList.add(new Option(args[i], null));
	                    } else {
	                    	optsList.add(new Option(args[i], args[i+1]));
	                    	i++;
	                    }
	                else {
	                	optsList.add(new Option(args[i], null));
	                }
	            }
	            break;
	        default:
	            // arg
	            argsList.add(args[i]);
	            break;
	        }
		}
		
		ClientAPI ca = null;
		String uri = null;
		int httpPort = 40010;
		
		for (Option o : optsList) {
			if (o.flag.equals("-h")) {
				// print help message
				System.out.println("-h    : print this help.");
				System.out.println("-p    : http server port.");
				
				System.out.println("-uri  : unified uri for SENTINEL and STANDALONE.");
			}
			if (o.flag.equals("-p")) {
				// set http server port
				if (o.opt == null) {
					System.out.println("-p HTTP_PORT");
					System.exit(0);
				}
				httpPort = Integer.parseInt(o.opt);
			}
			if (o.flag.equals("-uri")) {
				// parse uri
				if (o.opt == null) {
					System.out.println("-uri URI");
					System.exit(0);
				}
				uri = o.opt;
			}
		}
		
		if (uri == null) {
			System.out.println("Please use -uri to set service URI");
			System.exit(0);
		}
		
		try {
			ca = new ClientAPI();
			ca.init(uri);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		// Start the HTTP server
		Server server = new Server(httpPort);
		server.setHandler(new RestGatewayHandler(ca));
		try {
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}