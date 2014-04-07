/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package servermaster;

import java.net.*;
import java.io.*;
import java.util.ArrayList;

/**
 *
 * @author laurencewong
 */
public class ServerMaster {

    FileNode mFileRoot;
    ServerSocket mListenSocket;
    ArrayList<ServerMasterClientThread> mClients;

    public ServerMaster(int inSocketNum) {
        mFileRoot = new FileNode();
        mClients = new ArrayList<ServerMasterClientThread>();
        try {
            System.out.println("Starting server on port " + inSocketNum);
            mListenSocket = new ServerSocket(inSocketNum);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    public ServerMaster() {
        mFileRoot = new FileNode();
        mClients = new ArrayList<ServerMasterClientThread>();
        try {
            mListenSocket = new ServerSocket(6789);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    public void RunLoop() {
        String clientSentence;
        String capitalizedSentence;

        System.out.println("Starting server");
        try {
            while (true) {
                ServerMasterClientThread newClientThread = new ServerMasterClientThread(mListenSocket.accept(), this, mClients.size());
                newClientThread.start();
                mClients.add(newClientThread);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println(e.getLocalizedMessage());
        }
        System.out.println("Exiting server");
    }

    private class ServerMasterClientThread extends Thread {

        ServerMaster mMaster;
        Socket mClientSocket;
        int mClientNum;
        String clientSentence;
        String capitalizedSentence;

        public ServerMasterClientThread(Socket inClientSocket, ServerMaster inMaster, int inClientNum) {
            mClientNum = inClientNum;
            mMaster = inMaster;
            System.out.println("Creating new thread for client num: " + mClientNum);
            mClientSocket = inClientSocket;
        }

        public void run() {
            while (true) {
                try {
                    BufferedReader inFromClient
                            = new BufferedReader(new InputStreamReader(mClientSocket.getInputStream()));
                    DataOutputStream outToClient = new DataOutputStream(mClientSocket.getOutputStream());
                    clientSentence = inFromClient.readLine();
                    System.out.println("C:" + mClientNum + "Received: " + clientSentence);
                    if (clientSentence == "exit" || clientSentence == "Exit") {

                        System.out.println("Closing connection with client " + mClientNum);
                        mClientSocket.close();
                        return;

                    }
                    capitalizedSentence = clientSentence.toUpperCase() + '\n';
                    outToClient.writeBytes(capitalizedSentence);
                    this.wait();
                } catch (Exception e) {
//                    System.out.println(e.getMessage());
                }
            }
        }
    }
}
