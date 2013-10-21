package iie.mm.server;

import java.util.concurrent.BlockingQueue;

public class WriteThread implements Runnable {

	private ServerConf conf;
	private BlockingQueue<WriteTask> tasks;
	
	
	public WriteThread(ServerConf conf, BlockingQueue<WriteTask> tasks) {
		this.conf = conf;
		this.tasks = tasks;
	}


	@Override
	public void run() {
		StorePhoto sp = new StorePhoto(conf);
		
		try {
			while(true) {
				WriteTask t = tasks.take();
				
				if(t.getMd5() == null) {
					break;
				}
				synchronized (t) {
					t.setResult(sp.storePhoto(t.getSet(), t.getMd5(), t.getContent(), t.getCoff(), t.getClen()));
					t.notify();
				}
				
			}
			sp.close();
			System.out.println(Thread.currentThread()+"writethread 结束");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
