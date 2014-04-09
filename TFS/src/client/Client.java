/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package client;

import java.net.*;
import java.io.*;
import tfs.Message;
import tfs.MySocket;
/**
 *
 * @author laurencewong
 */
public class Client {

    String mServerIp;
    int mServerPortNum;

    /**
     *
     * @param mServerInfo a string that MUST be in the format ip:port
     */
    public Client(String mServerInfo) {
        String[] parts = mServerInfo.split(":");
        if (parts.length != 2) {
            System.out.println("Server information " + mServerInfo + " is in wrong format");
            return;
        }

        mServerIp = parts[0];
        mServerPortNum = Integer.valueOf(parts[1]);
    }

    public void InitConnectionWithServer(MySocket inSocket) {
        try {
            System.out.println("Telling server that I am a client");
            Message toServer = new Message();
            toServer.WriteString("Client");
            inSocket.WriteMessage(toServer);
        } catch (IOException ioExcept) {
            System.out.println("Problem initializing connection with server");
            System.out.println(ioExcept.getMessage());
        }
    }

    /**
     *
     */
    public void RunLoop() {
        String sentence = "";
        String modifiedSentence = "";
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        MySocket clientSocket;
        System.out.println("Starting client on ip: " + mServerIp + " and port: " + mServerPortNum);
        while (true) {
            try {
                clientSocket = new MySocket(mServerIp, mServerPortNum);
                
                InitConnectionWithServer(clientSocket);
                
                while (true) {
                    
                    Message toServer = new Message();
                    

                    if (inFromUser.ready()) {
                        sentence = inFromUser.readLine();
                    }

                    if (false /*eventually add heartbeat message check in here*/) {
                        break;
                    }

                    if (!sentence.isEmpty()) {
                        toServer.WriteString(sentence);
                        clientSocket.WriteMessage(toServer);
                    }
                    if (clientSocket.hasData()) {
                        Message fromServer = new Message(clientSocket.ReadBytes());
                        String response = fromServer.ReadString();
                        System.out.println("FROM SERVER: " + response);
                        //hacky temp code
                        ContactChunkServer(fromServer.ReadString(), sentence);

                    }
                    sentence = "";

                }
                clientSocket.close();
            } catch (IOException e) {
                if (e.getMessage().contentEquals("Connection refused")) {
                    System.out.println("Server is not online.  Attempting to reconnect in 3s");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e1) {
                        System.out.println("Exception sleeping");
                        System.out.println(e1.getMessage());
                    }
                }
            }
        }
    }

    public void ContactChunkServer(String input, String in) {
        MySocket chunkServerSocket = null;
        System.out.println("Contacting chunk server with param " + input);
        if (input.contentEquals("localhost:6999")) {
            String[] splitInput = input.split(":");
            try {
                Message toChunkServer = new Message();
                System.out.println("Connecting to chunkServer at: " + input);
                chunkServerSocket = new MySocket(splitInput[0], Integer.valueOf(splitInput[1]));

                InitConnectionWithServer(chunkServerSocket);
                
                //outToServer.flush();

                //String toChunk = "ReadFile";
                toChunkServer.WriteString("ReadFile");
                chunkServerSocket.WriteMessage((toChunkServer));

                //chunkServerSocket.close();
            } catch (Exception e) {
                System.out.println("Problem writing byte");
                System.out.println(e.getMessage());
            }
        }
    }

}
