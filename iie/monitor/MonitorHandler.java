package iie.monitor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;

public class MonitorHandler extends AbstractHandler {

	private void badResponse(Request baseRequest, HttpServletResponse response,
			String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		baseRequest.setHandled(true);
		response.getWriter().println(message);
		response.getWriter().flush();
	}

	private void doDataCount(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)throws IOException, ServletException {
		// System.out.println(target);
		if (target.equalsIgnoreCase("/datacount/main.html")) {
			String city = request.getParameter("city");
			String date = request.getParameter("date");
			String error = runCmd("cd datacount; ./doplot.sh report/"+city+"/report-"+date);
			if(error != null)
			{
				badResponse(baseRequest, response, "#FAIL: "+error);
				return;
			}
		}
		ResourceHandler rh = new ResourceHandler();
		rh.setResourceBase(".");
		rh.handle(target, baseRequest, request, response);
	}

	private String runCmd(String cmd) throws IOException {
		Process p = Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", cmd });
		try {
			InputStream err = p.getErrorStream();
			InputStreamReader isr = new InputStreamReader(err);
			BufferedReader br = new BufferedReader(isr);
			String error = null;
			String line = null;
			while ((line = br.readLine()) != null)
			{
				System.out.println(line);
				error += line + "\n";
			}
			int exitVal = p.waitFor();
			if (exitVal > 0)
				return error;
			else
				return null;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void handle(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		// TODO Auto-generated method stub
		if (target == null) {
			badResponse(baseRequest, response, "#FAIL:taget can not be null");
		} else if (target.startsWith("/datacount/")) {
			doDataCount(target, baseRequest, request, response);
		} else {
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		}

	}

}
