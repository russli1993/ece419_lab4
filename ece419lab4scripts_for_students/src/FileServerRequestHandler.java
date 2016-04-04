import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class FileServerRequestHandler implements Runnable {

	private String dictDir = "lowercase.rand";
	private MSocket mSocket = null;

	public FileServerRequestHandler(Socket socket) throws IOException {
		this.mSocket = new MSocket(socket);
	}

    private ArrayList<String> getPartition(int partitionId) {
    	try {
        	BufferedReader br = new BufferedReader(new FileReader(this.dictDir));
        	String str = null;
        	ArrayList<String> lines = new ArrayList<>();
        	while ((str = br.readLine()) != null) {
        		lines.add(str);
        	}
        	br.close();
        	
        	ArrayList<String> partition = new ArrayList<String>();
        	int start = partitionId * lines.size();
        	int end = (partitionId + 1) * lines.size();
        	for (int i = start; i < end; i++) {
        		partition.add(lines.get(i));
        	}
        	return partition;
    	}
    	catch (IOException e) {
    		e.printStackTrace();
    		return null;
    	}
    }

	@Override
	public void run() {
		try {
			Logger.print("Worker thread connected");
			MPacket packet = this.mSocket.readMPacket();
			ArrayList<String> partition = getPartition(packet.partitionId);
			this.mSocket.writeMPacket(packet);
			this.mSocket.close();
			return; // job done. useless now.
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
