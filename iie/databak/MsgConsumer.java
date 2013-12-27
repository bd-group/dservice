package iie.databak;

import java.util.concurrent.Executor;

import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class MsgConsumer {

	final MetaClientConfig metaClientConfig = new MetaClientConfig();
	final ZKConfig zkConfig = new ZKConfig();
	private String addr;

	public MsgConsumer(String addr) {
		this.addr = addr;
	}
	public void consume() throws MetaClientException {
		// 设置zookeeper地址
		zkConfig.zkConnect = addr;
		metaClientConfig.setZkConfig(zkConfig);
		// New session factory,强烈建议使用单例
		MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
		final String topic = "meta-test";
		final String group = "meta-databak";
		// create consumer,强烈建议使用单例

		// 生成处理线程
		MessageConsumer consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
		// 订阅事件，MessageListener是事件处理接口
		consumer.subscribe(topic, 1024, new MessageListener() {

			@Override
			public Executor getExecutor() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void recieveMessages(final Message message) {
				String data = new String(message.getData());

				System.out.println(data);
				DDLMsg msg = DDLMsg.fromJson(data);
				// if(msg.getLocalhost_name().equals(localhost_name))
				// {
				// LOG.info("---zy--local msg,no need to refresh " );
				// // handler.refresh(msg);
				// }
				// else
				// just test
				// handler.refresh(msg);
			}

		});
		consumer.completeSubscribe();
	}

	public static void main(String[] args) {
		String addr = "192.168.1.13:3181";
		if(args.length >= 1)
		{
			addr = args[0];
		}
		else
		{
//			System.out.println("please provide addr at zookeeper");
//			System.exit(0);
		}
		try {
			new MsgConsumer(addr).consume();
		} catch (MetaClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
