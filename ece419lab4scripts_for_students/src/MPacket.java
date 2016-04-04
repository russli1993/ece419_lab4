import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;



public class MPacket implements Serializable {

	// for ClientDriver to FileServer, and vise versa
	public String jobId;
	public String jobType;
	public String jobResult;
	
	// for transporting any sort of IP
	public String host;
	public int port;
	
	public int partitionId;
	public HashMap<String, String> data;
	
	public Request requestType;

	public static byte[] serialize(Object obj) {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    try {
	    	ObjectOutputStream os = new ObjectOutputStream(out);
	    	os.writeObject(obj);
	    	return out.toByteArray();
	    }
	    catch (Exception e) {
	    	Logger.print("Cannot serialize object " + obj.toString());
	    	return null;
	    }
	}
	public byte[] serialize() {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    try {
	    	ObjectOutputStream os = new ObjectOutputStream(out);
	    	os.writeObject(this);
	    	return out.toByteArray();
	    }
	    catch (Exception e) {
	    	Logger.print("Cannot serialize object " + this.toString());
	    	return null;
	    }
	}
	public static MPacket deserialize(byte[] data) {
		try {
			ByteArrayInputStream in = new ByteArrayInputStream(data);
			ObjectInputStream is = new ObjectInputStream(in);
			return (MPacket)is.readObject();
		}
	    catch (Exception e) {
	    	Logger.print("Cannot deserialize object");
	    	return null;
	    }
	}
}
