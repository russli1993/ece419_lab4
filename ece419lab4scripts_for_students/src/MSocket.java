import java.net.Socket;
import java.io.StreamCorruptedException;
import java.io.OptionalDataException;
import java.io.IOException;
import java.io.EOFException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;

public class MSocket{
    /*
     * This class is used as a wrapper around sockets and streams.
     * In addition to allowing network communication,
     * it tracks statistics and can add delays and packet reordering.
     */

    /*************Member objects for communication*************/
    private Socket socket = null;
    private ObjectInputStream in = null;
    private ObjectOutputStream out = null;


    //The queue of packets to send
    private BlockingQueue<Object> egressQueue = null;
    //The queue of packets received
    private BlockingQueue<Object> ingressQueue = null;

    
    /*************Helper Classes*************/
    /*
     *The following inner class asynchronously
     *receives packets and adds it to the ingressQueue
     */
     class Receiver implements Runnable{
     
        public void run(){
            try{
                
                Object incoming = in.readObject();
                //if(Debug.debug) System.out.println("Number of packets received: " + ++rcvdCount);
                //if(Debug.debug) System.out.println("Received packet: " + incoming);
                while(incoming != null){
                    ingressQueue.put(incoming);
                    incoming = in.readObject();
                }
            }catch(StreamCorruptedException e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }catch(OptionalDataException e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }catch(EOFException e){
                e.printStackTrace();
                close();
            }catch(IOException e){
                e.printStackTrace();
            }catch(ClassNotFoundException e){
                e.printStackTrace();
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }
     }
    
    /*
     *The following inner class sends packets by reordering them 
     and adding a delay. 
     There are two ways to do this: 1) when a thread wakes up, it 
     sends ALL packets in an order determined by UNORDER_FACTOR;
     2) when a thread wakes up it sends ONE packet based on UNORDER_FACTOR. 
     This implementation uses the former. 
    */
    class NetworkErrorSender implements Runnable{
        
        public void run(){
            try{
                ArrayList<Object> events = new ArrayList<Object>();

                //Drain the entire egress queue into the events list
                Object head = egressQueue.poll();
                while(head != null){
                    events.add(head);    
                    head = egressQueue.poll();
                }

                //Now send all the events
                while(events.size() > 0){
                    //if(Debug.debug) System.out.println("Number of packets sent: " + ++sentCount);
                    //Need to synchronize on the ObjectOutputStream instance; otherwise
                    //multiple writes may corrupt stream and/or packets
                    synchronized(out) {
                        out.writeObject(events.remove(0));
                        out.flush();
                        out.reset();
                    }
                }

            }catch(IOException e){
                e.printStackTrace();
            }
        }
    }

    /*************Constructors*************/
    /*
     *This creates a regular socket
     */
    public MSocket(String host, int port) throws IOException{
        socket = new Socket(host, port);
        //NOTE: outputStream should be initialized before
        //inputStream, otherwise it will block
        out = new ObjectOutputStream(socket.getOutputStream());
        in = new ObjectInputStream(socket.getInputStream());
        
        egressQueue = new LinkedBlockingQueue<Object>();
        ingressQueue = new LinkedBlockingQueue<Object>();
        //Start the receiver thread
        //NOTE: This will keep updating the ingress queue
        (new Thread(new Receiver())).start();
    }

    //Similar to above, except takes an initialized socket
    //NOTE: This constructor is for internal use only
    public MSocket(Socket soc) throws IOException{
        socket = soc;

        out = new ObjectOutputStream(socket.getOutputStream());
        in = new ObjectInputStream(socket.getInputStream());
        
        egressQueue = new LinkedBlockingQueue<Object>();
        ingressQueue = new LinkedBlockingQueue<Object>();

        (new Thread(new Receiver())).start();
    }

    
    /*************Public Methods*************/

    //This method is for reference and testing
    //it reads objects without inducing network errors
    //NOTE: THIS SHOULD NOT BE USED IN YOUR SOLUTION
    public synchronized MPacket readMPacket() throws IOException{
        Object incoming = null;
        try{
            incoming = ingressQueue.take();
            return (MPacket)incoming;
        }catch(InterruptedException e){
            e.printStackTrace();
            return null;
        }
    }

    //This method is for reference and testing
    //it writes objects without inducing network errors
    //NOTE: THIS SHOULD NOT BE USED IN YOUR SOLUTION
    public void writeMPacket(Object o){
        try{
            out.writeObject(o);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    //Closes network objects, i.e. sockets, InputObjectStreams, 
    // OutputObjectStream
    public void close() {
        try{
            in.close();
            out.close();
            socket.close();
         }catch(IOException e){
         }
    }

}
