import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;


public class Request {
	public static int port = 49152;
	private int serialNo;
	private String message;

	Request(int serialNo, String message) {
		this.serialNo = serialNo;
		this.message = message;
	}

	public String sendRequest(int c) throws UnknownHostException, IOException, ClassNotFoundException {
		Socket s = new Socket("127.0.0.1", port + this.serialNo); 

		ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
		out.writeObject(this.message);
		out.flush();

		// write table
		out.writeObject(c);
		out.flush();

		s.close();
		return null;
	}
	
	public String sendReceiveRequest() throws UnknownHostException, IOException, ClassNotFoundException {
		Socket s = new Socket("127.0.0.1", port + this.serialNo); 

		ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
		out.writeObject(this.message);
		out.flush();

		String response = new String();
		ObjectInputStream in = new ObjectInputStream(s.getInputStream());
		response = (String) in.readObject();
		s.close();
		return response;
	}
	
	public Hashtable<String, String> queryRequest() throws UnknownHostException, IOException, ClassNotFoundException{
		Socket s = new Socket("127.0.0.1", port + this.serialNo);

		ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
		out.writeObject(this.message);
		out.flush();

		Hashtable<String, String> response;
		ObjectInputStream in = new ObjectInputStream(s.getInputStream());
		response = (Hashtable<String, String>) in.readObject();
		s.close();
		return response;
		
	}
	
	public int IdRequest() throws UnknownHostException, IOException, ClassNotFoundException{
		Socket s = new Socket("127.0.0.1", port + this.serialNo);

		ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
		out.writeObject(this.message);
		out.flush();

		int response;
		ObjectInputStream in = new ObjectInputStream(s.getInputStream());
		response = (int) in.readObject();
		s.close();
		return response;
		
	}
	
	public void writeRequest(Hashtable<String, String> table) throws UnknownHostException, IOException{
		Socket s = new Socket("127.0.0.1", port + this.serialNo);

		ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
		out.writeObject(this.message);
		out.flush();
	
		// write table
		out.writeObject(table);
		out.flush();
		s.close();
	}
	
	public String NodeIdRequest() throws UnknownHostException, IOException, ClassNotFoundException {
		Socket s = new Socket("127.0.0.1", port + this.serialNo);

		ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
		out.writeObject(this.message);
		out.flush();

		String response;
		ObjectInputStream in = new ObjectInputStream(s.getInputStream());
		response = (String) in.readObject();
		s.close();
		return response;
	}
}
