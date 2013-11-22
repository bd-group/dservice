package iie.mm.server;


//给客户端提供了get和search两种方法读取图片,但是到服务端都对应的是search

public class ActionType {
	public static final byte SYNCSTORE = 1;
	public static final byte SEARCH = 2;
	public static final byte DELSET = 3;
	public static final byte ASYNCSTORE = 4;
	public static final byte SERVERINFO = 5;
	public static final byte IGET = 6;
	public static final byte MPUT = 7;
}
