package iie.mm.server;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;

import java.io.IOException;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class HTTPHandler extends AbstractHandler
{
	private ServerConf conf;
	private StorePhoto sp;
	public HTTPHandler(ServerConf conf)
	{
		this.conf = conf;
		sp = new StorePhoto(conf);
	}
	public void handle(String target,Request baseRequest,HttpServletRequest request,HttpServletResponse response)
	{
//		System.out.println(request.getQueryString());
		System.out.println(request.getPathInfo() );
		try{
			String key = request.getParameter("key");
			if(key == null)
			{
				response.setContentType("text/html;charset=utf-8");
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				baseRequest.setHandled(true);
				response.getWriter().println("#FAIL:key can not be null");
			}
			else
			{
				System.out.println(key);
				String[] infos = key.split("#");		
				if(infos.length == 2)
				{
					byte[] content = sp.getPhoto(infos[0],infos[1]);
					if(content == null || content.length == 0)
					{
						response.setContentType("text/html;charset=utf-8");
						response.setStatus(HttpServletResponse.SC_NOT_FOUND);
						baseRequest.setHandled(true);
						response.getWriter().println("#FAIL:can not find any photo by key:"+key);
						
					}
					else
					{
						response.setContentType("image");
						response.setStatus(HttpServletResponse.SC_OK);
						baseRequest.setHandled(true);
						response.getOutputStream().write(content);
						response.getOutputStream().flush();
					}
				}
				else if(infos.length == 8)
				{
					byte[] content = sp.searchPhoto(key);
					if(content == null || content.length == 0)
					{
						response.setContentType("text/html;charset=utf-8");
						response.setStatus(HttpServletResponse.SC_NOT_FOUND);
						baseRequest.setHandled(true);
						response.getWriter().println("#FAIL:can not find any photo by key:"+key);
						
					}
					else
					{
						response.setContentType("image");
						response.setStatus(HttpServletResponse.SC_OK);
						baseRequest.setHandled(true);
						response.getOutputStream().write(content);
						response.getOutputStream().flush();
					}
				}
				else{
					response.setContentType("text/html;charset=utf-8");
					response.setStatus(HttpServletResponse.SC_BAD_REQUEST );
					baseRequest.setHandled(true);
					response.getWriter().println("#FAIL:wrong format of key");
				}
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	     
}