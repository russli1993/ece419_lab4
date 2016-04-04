import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;



public class FileServer {
	
	private ZkConnector zkc = null;
	private ZooKeeper zooKeeper = null;
	private Watcher watcher = null;
	private String fsPath = "/file_server";
	private String dictDir = "";

	public String host = null;
	public int port = -1;
	private ServerSocket mServerSocket = null;

	public FileServer(String connectString, String dictDir) throws IOException {
		this.dictDir = dictDir;
        this.zkc = new ZkConnector();
        try {
            zkc.connect(connectString);
        } catch(Exception e) {
        	Logger.print("Zookeeper connect "+ e.getMessage());
        }
        // sets up zookeeper and watchers
        this.zooKeeper = zkc.getZooKeeper();
        watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
            }
        };

		this.mServerSocket = new ServerSocket(0);
		this.port = mServerSocket.getLocalPort();
		this.host = InetAddress.getLocalHost().getHostAddress();
	}

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(fsPath)) {
            if (type == EventType.NodeDeleted) {
                Logger.print(fsPath + " deleted! Let's go!");       
            }
            else if (type == EventType.NodeCreated) {
            	Logger.print(fsPath + " created!");
            }
            else if (type == EventType.NodeDataChanged) {
            	Logger.print(fsPath + " data changed! None of my business!");
            }
            else {
            	Logger.print(fsPath + " something else happened!");
            }
            checkpath();
        }
    }

    private void checkpath() {
		Stat stat = zkc.exists(fsPath, watcher);
		if (stat == null) { // znode doesn't exist; let's try creating it
			Logger.print("Creating " + fsPath);
			Code ret = zkc.create(fsPath, // Path of znode
					null, // Data not needed.
					CreateMode.EPHEMERAL // Znode type, set to EPHEMERAL.
					);
			if (ret == Code.OK) {
				Logger.print("the PRIMARY!");
				serveRequests();
			}
        }
    }

    // Serve requests from worker minions
    private void serveRequests() {
		MPacket packet = new MPacket();
		packet.host = this.host;
		packet.port = this.port;
		try {
			this.zooKeeper.setData(fsPath, MPacket.serialize(packet), -1);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}

		Logger.print("Serving worker requests");
		try {
			while (true) {
				Socket socket = this.mServerSocket.accept();
				FileServerRequestHandler handler = new FileServerRequestHandler(socket, this.dictDir);
				new Thread(handler).start();
			}
    	}
    	catch (IOException e) {
    		e.printStackTrace();
    	}
    }



	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			Logger.print("Wrong input format. Usage: FileServer host:port dictionary_dir");
			return;
		}

		FileServer fs = new FileServer(args[0], args[1]);

		fs.checkpath();
        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }
	}
}