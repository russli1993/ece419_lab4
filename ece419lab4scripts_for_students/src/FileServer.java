import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

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
	}

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(fsPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(fsPath + " deleted! Let's go!");       
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(fsPath + " created!");
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }

    private void checkpath() {
		Stat stat = zkc.exists(fsPath, watcher);
		if (stat == null) { // znode doesn't exist; let's try creating it
			System.out.println("Creating " + fsPath);
			Code ret = zkc.create(fsPath, // Path of znode
					null, // Data not needed.
					CreateMode.EPHEMERAL // Znode type, set to EPHEMERAL.
					);
			if (ret == Code.OK) {
				System.out.println("the PRIMARY!");
				
				FileServerListener listener = new FileServerListener();
				MPacket packet = new MPacket();
				packet.host = listener.host;
				packet.port = listener.port;
				new Thread(listener).start();
				try {
					this.zooKeeper.setData(fsPath, MPacket.serialize(packet), -1);
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
			}
        } 
    }

	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			Logger.print("Wrong input format. Usage: FileServer host:port dictionary_dir");
		}

		FileServer fs = new FileServer(args[0], args[1]);
        fs.checkpath();
        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }
	}
}