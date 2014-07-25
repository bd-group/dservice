package iie.monitor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;

public class MonitorHandler extends AbstractHandler {

	private HashMap<String, String> cityip = new HashMap<String, String>();
	private HashMap<String, String> mmcityip = new HashMap<String, String>();
	private ConcurrentHashMap<String, Long> uidtime = new ConcurrentHashMap<String, Long>();
	private String targetPath, mmPath, redisPath;
	
	public MonitorHandler(Map<String, String> addrMap, Map<String, String> mmAddrMap, String targetPath, String mmPath, String redisPath) {
		cityip.putAll(addrMap);
		mmcityip.putAll(mmAddrMap);
		this.targetPath = targetPath;
		this.mmPath = mmPath;
		this.redisPath = redisPath;
		Timer t = new Timer();
		t.schedule(new EvictThread(10 * 1000), 10 * 1000, 10 * 1000);
	}
	
	private class EvictThread extends TimerTask {
		private long expireTime;	//单位ms
		
		public EvictThread(long expireTime) {
			this.expireTime = expireTime;
		}

		@Override
		public void run() {
			Set<String> keys = new HashSet<String>();
			for (Map.Entry<String, Long> en : uidtime.entrySet()) {
				if (System.currentTimeMillis() - en.getValue() > expireTime) {
					keys.add(en.getKey());
					if (!(en.getKey().equalsIgnoreCase("") || en.getKey().contains("*") || en.getKey().contains("."))) {
						try {
							runCmd("rm -rf datacount/" + en.getKey());
							System.out.println("rm -rf datacount/" + en.getKey());
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
			for(String s : keys) {
				uidtime.remove(s);
			}
		}
	}
	
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
		String city = request.getParameter("city");
		String date = request.getParameter("date");
		String id = request.getParameter("id");
		if (target.equals("/monitor/main.html")) {
			if (city != null && date != null && id != null &&
					!city.equalsIgnoreCase("") &&
					!date.equalsIgnoreCase("") && 
					!id.equalsIgnoreCase("")) {
				uidtime.put(id, System.currentTimeMillis());
				String name = "report-" + date;
				String cmd = "cd monitor;"
						+ "expect data.exp " + cityip.get(city) + " " + targetPath + "/" + name + " " + id + "/" + city + "." + name + ";"
						+ " ./doplot.sh " + id + "/" + city + "." + name + " " + id + "/";
				System.out.println(cmd);
				String error = runCmd(cmd);
				if (error != null) {
					badResponse(baseRequest, response, "#FAIL: "+error);
					return;
				}
				if (mmcityip.get(city) != null) {
					cmd = "cd monitor;"
							+ "expect data.exp " + mmcityip.get(city) + " " + mmPath + "/sysinfo-" + date + " " + id + "/" + city + ".sysinfo-" + date + ";"
							+ " ./doplot2.sh " + id + "/ " + id  + "/" + city + ".sysinfo-" + date;
					System.out.println(cmd);
					error = runCmd(cmd);
					if (error != null) {
						badResponse(baseRequest, response, "#FAIL: "+error);
						return;
					}
				} else {
					cmd = "cd monitor;"
							+ "rm -rf " + id + "/mms_*.png";
					System.out.println(cmd);
					runCmd(cmd);
				}
				{
					cmd = "cd monitor;" 
							+ "expect data.exp " + cityip.get(city) + " " + redisPath + "/redisinfo-" + date + " " + id + "/" + city + ".redisinfo-" + date + ";"
							+ " ./doplot3.sh " + id + "/ " + id + "/" + city + ".redisinfo-" + date;
					System.out.println(cmd);
					error = runCmd(cmd);
					if (error != null) {
						// ignore any error here
						System.out.println("redis info plot failed， ignore it!");
						cmd = "cd monitor;"
								+ "rm -rf " + id + "/redis_*.png";
						System.out.println(cmd);
						runCmd(cmd);
					}
				}
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
			while ((line = br.readLine()) != null) {
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
		if (target == null) {
			badResponse(baseRequest, response, "#FAIL:taget can not be null");
		} else if (target.startsWith("/monitor/")) {
			doDataCount(target, baseRequest, request, response);
		} else {
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		}
	}
}
