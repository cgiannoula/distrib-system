import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;


public class MainQuery {

	public static void main(String[] args) {
		try {
			// Read from file-inserts
			BufferedReader br = new BufferedReader(new FileReader(args[0]));
			ArrayList<String> readList = new ArrayList<String>();
			String currLine;
			while ((currLine = br.readLine()) != null)
				readList.add(currLine);

			// Read from file-queries
			br = new BufferedReader(new FileReader(args[1]));
			ArrayList<String> queryList = new ArrayList<String>();
			while ((currLine = br.readLine()) != null)
				queryList.add(currLine);

			// Open a file for writing
			PrintWriter writer = new PrintWriter(args[2]);

			// Add "insert" in inputs
			currLine = new String("insert, ");
			int size = readList.size();
			for (int i = 0; i < size; i++) {
				String curr = readList.remove(0);
				curr = curr.concat("\n");
				currLine = currLine.concat(curr);
				readList.add(currLine);
				currLine = new String("insert, ");
			}

			// Add "query" in inputs
			currLine = new String("query, ");
			int sizeQ = queryList.size();
			for (int i = 0; i < sizeQ; i++) {
				String curr = queryList.remove(0);
				curr = curr.concat("\n");
				currLine = currLine.concat(curr);
				queryList.add(currLine);
				currLine = new String("query, ");
			}

			System.out.println(sizeQ);

			// Create a Chord with 10 nodes
			Chord chord = new Chord();
			for (int i = 1; i <= 10; i++)
				chord.createNode(i);
			chord.setNeighbours();

			writer.println("Responses to client:");
			// Do the insertions
			for (int i = 0; i < size; i++) {
				int r = randInt(1, 10) + 49152;

				Socket s = new Socket("127.0.0.1", r); ServerSocket p = new ServerSocket(49180); 
				// Send the message to the server
				
				ObjectOutputStream out = new ObjectOutputStream(
						s.getOutputStream());
				out.writeObject(readList.get(i));
				out.flush();
				out.writeObject(49180);
				out.flush();
				s.close();
				
				Socket incoming = p.accept();
				String response;
				ObjectInputStream in = new ObjectInputStream(incoming.getInputStream());
				response = (String) in.readObject();
				p.close();
			}
			
			
			long startTime = System.currentTimeMillis();
			// Do the queries
			for (int i = 0; i < sizeQ; i++) {
				int r = randInt(1, 10) + 49152;

				Socket s = new Socket("127.0.0.1", r);
				// Send the message to the server

				ObjectOutputStream out = new ObjectOutputStream(
						s.getOutputStream());
				out.writeObject(queryList.get(i));
				out.flush();
				out.writeObject(49190);
				out.flush();
				s.close();
				ServerSocket p = new ServerSocket(49190); 
				Socket incoming = p.accept();
				Hashtable<String, String> responseQ;
				ObjectInputStream in = new ObjectInputStream(incoming.getInputStream());
				responseQ = (Hashtable<String, String>) in.readObject();
				writer.print(i + " ");
				writer.println(responseQ);
				p.close();
			}

			long endTime = System.currentTimeMillis();
			long miliseconds = (endTime - startTime);

			System.out.println(miliseconds);
			writer.println();
			writer.println("----------------------------------------");
			writer.println("Time in miliseconds: " + miliseconds);
			writer.println();

			writer.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * Produce random integers in range of [min,max]
	 */
	public static int randInt(int min, int max) {
		Random rand = new Random();
		int randomNum = rand.nextInt((max - min) + 1) + min;

		return randomNum;
	}

}
