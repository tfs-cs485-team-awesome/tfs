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
                        String[] sentenceTokenized = sentence.split(" ");
                        for (String s : sentenceTokenized) {
                            toServer.WriteString(s);
                        }

                        clientSocket.WriteMessage(toServer);
                    }
                    if (clientSocket.hasData()) {
                        Message fromServer = new Message(clientSocket.ReadBytes());
                        while(!fromServer.isFinished()) {
                            //message has data
                            ParseInput(fromServer);
                        }

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
    
    public void ParseInput(Message m) {
        String input = m.ReadString();
        switch(input) {
            case "Print":
                //print out a debug statement
                System.out.println(m.ReadString());
                break;
            case "ChunkInfo":
                ContactChunkServer(m);
                break;
        }
    }

    public void ContactChunkServer(Message m) {
        MySocket chunkServerSocket = null;
        String chunkServerInfo = m.ReadString();
        System.out.println("Contacting chunk server with param " + chunkServerInfo);
        if (chunkServerInfo.contentEquals("localhost:6999")) {
            String[] splitInput = chunkServerInfo.split(":");
            try {
                Message toChunkServer = new Message();
                System.out.println("Connecting to chunkServer at: " + chunkServerInfo);
                chunkServerSocket = new MySocket(splitInput[0], Integer.valueOf(splitInput[1]));

                InitConnectionWithServer(chunkServerSocket);
                
                toChunkServer.WriteString("ReadFile");
                chunkServerSocket.WriteMessage((toChunkServer));

                chunkServerSocket.close();
            } catch (Exception e) {
                System.out.println("Problem writing byte");
                System.out.println(e.getMessage());
            }
        }
    }
}
