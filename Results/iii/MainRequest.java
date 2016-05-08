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

import javax.swing.text.html.HTMLDocument.Iterator;

public class MainRequest {

	public static void main(String[] args) {
		try {
			// Read from file-inserts
			BufferedReader br = new BufferedReader(new FileReader(args[0]));
			ArrayList<String> readList = new ArrayList<String>();
			String currLine;
			while ((currLine = br.readLine()) != null)
				readList.add(currLine);

			// Open a file for writing
			PrintWriter writer = new PrintWriter(args[1]);

			// Create a Chord with 10 nodes
			Chord chord = new Chord();
			for (int i = 1; i <= 10; i++)
				chord.createNode(i);
			chord.setNeighbours();

			writer.println("Responses to client:");
			int size = readList.size();
			// Do the insertions
			for (int i = 0; i < size; i++) {
				int r = randInt(1, 10) + 49152;

				Socket s = new Socket("127.0.0.1", r);
				// Send the message to the server

				ObjectOutputStream out = new ObjectOutputStream(
						s.getOutputStream());
				out.writeObject(readList.get(i));
				out.flush();
				out.writeObject(49180);
				out.flush();
				s.close();
				ServerSocket p = new ServerSocket(49180); 
				Socket incoming = p.accept();
				ObjectInputStream in = new ObjectInputStream(incoming.getInputStream());
				Object response = in.readObject();
				writer.print(i + " ");
				writer.println(response);
				p.close();
			}

			writer.println();
			writer.println("----------------------------------------");
			System.out.println("----------------------------------------");
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
