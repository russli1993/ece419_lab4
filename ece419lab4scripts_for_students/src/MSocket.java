import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;


public class MSocket {
	private ObjectOutputStream out;
	private ObjectInputStream in;
	private Socket socket;
	
	public MSocket(String host, int port) throws UnknownHostException, IOException {
		this.socket = new Socket(host, port);
		this.out = new ObjectOutputStream(socket.getOutputStream());
		this.in = new ObjectInputStream(socket.getInputStream());
	}


	public MSocket(Socket socket) throws UnknownHostException, IOException {

		this.socket = socket;
		this.out = new ObjectOutputStream(socket.getOutputStream());
		this.in = new ObjectInputStream(socket.getInputStream());
	}
	
	public void writeMPacket(MPacket obj) throws IOException {
		out.writeObject(obj);
		Logger.print("Socket delivered a packet");
	}

	public MPacket readMPacket() throws IOException {
		try {
			return (MPacket)in.readObject();
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void close() {
		try {
			this.socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
