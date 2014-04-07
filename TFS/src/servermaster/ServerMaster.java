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
    ArrayList<Socket> mClients;
    
    
    public ServerMaster(int inSocketNum) {
        mFileRoot = new FileNode();
        
        try{
        mListenSocket = new ServerSocket(inSocketNum);
        } catch (Exception e)
        {
            System.out.println(e.getLocalizedMessage());
        }
    }
        public ServerMaster() {
        mFileRoot = new FileNode();

        try {
            mListenSocket = new ServerSocket(6879);
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
                Socket connectionSocket = mListenSocket.accept();
                BufferedReader inFromClient
                        = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
                clientSentence = inFromClient.readLine();
                System.out.println("Received: " + clientSentence);
                capitalizedSentence = clientSentence.toUpperCase() + '\n';
                outToClient.writeBytes(capitalizedSentence);
            }
        } catch (Exception e) {
        }
    }
}
