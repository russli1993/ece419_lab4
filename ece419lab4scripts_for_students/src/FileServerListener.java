import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;


public class FileServerListener implements Runnable {

	public ServerSocket mServerSocket = null;
	public String host;
	public int port;

	public FileServerListener() {
		try {
			mServerSocket = new ServerSocket();
			this.port = mServerSocket.getLocalPort();
			this.host = InetAddress.getLocalHost().getHostAddress();
		} catch (IOException e) {
			// TODO what do with IOException?
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
        try {
			mServerSocket.accept();
			Logger.print("File Server connected");
		} catch (IOException e) {
			// TODO: what do with IOException?
			e.printStackTrace();
		}
	}

}
//incomingMServerSocket = new MServerSocket(0);
//int localPort = incomingMServerSocket.getLocalPort();
//serverSocket = new Socket(serverHost, serverPort);
////Send hello packet to server
//MPacket hello = new MPacket(name, MPacket.HELLO, MPacket.HELLO_INIT);
//hello.mazeWidth = mazeWidth;
//hello.mazeHeight = mazeHeight;
//hello.selfPort = localPort;
//hello.selfHost = InetAddress.getLocalHost().getHostAddress(); // TODO: might not work on LAN