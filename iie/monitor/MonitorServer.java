package iie.monitor;

import org.eclipse.jetty.server.Server;

public class MonitorServer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int port = 33333;
		if(args.length == 0)
		{
			System.out.println("use default port: "+port);
		}
		else{			
			try{
				port = Integer.parseInt(args[0]);
				
			}catch(NumberFormatException e)
			{
				System.out.println("wrong format of port:"+args[0]);
				System.exit(0);
			}
		}
		Server s = new Server(port);
		s.setHandler(new MonitorHandler());
		try {
			s.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
