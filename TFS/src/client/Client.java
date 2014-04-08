/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package client;

import java.net.*;
import java.io.*;

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

    public void InitConnectionWithServer(DataOutputStream outToServer, BufferedReader inFromServer) {
        try {

            outToServer.writeBytes("Client"); //tell server i am a client

        } catch (IOException ioExcept) {
            System.out.println("Problem initializing connection with server");
            System.out.println(ioExcept.getMessage());
        }
    }

    /**
     *
     */
    public void RunLoop() {
        String sentence;
        String modifiedSentence;
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        Socket clientSocket;
        System.out.println("Starting client on ip: " + mServerIp + " and port: " + mServerPortNum);
        while (true) {
            try {
                clientSocket = new Socket(mServerIp, mServerPortNum);
                DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
                BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                InitConnectionWithServer(outToServer, inFromServer);
                while (true) {

                    sentence = inFromUser.readLine();

                    if (false /*eventually add heartbeat message check in here*/) {
                        break;
                    }

                    outToServer.writeBytes(sentence + '\n');
                    modifiedSentence = inFromServer.readLine();
                    System.out.println("FROM SERVER: " + modifiedSentence);
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

}
