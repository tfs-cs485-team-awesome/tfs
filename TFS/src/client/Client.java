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
    public Client(String mServerInfo)
    {
        String[] parts = mServerInfo.split(":");
        if(parts.length != 2)
        {
            System.out.println("Server information " + mServerInfo + " is in wrong format");
            return;
        }
        
        mServerIp = parts[0];
        mServerPortNum = Integer.valueOf(parts[1]);
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
        try {
            clientSocket = new Socket(mServerIp, mServerPortNum);
            while (true) {
                DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
                BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                sentence = inFromUser.readLine();
                
                if(sentence.contentEquals("quit"))
                {
                    break;
                }
                
                outToServer.writeBytes(sentence + '\n');
                modifiedSentence = inFromServer.readLine();
                System.out.println("FROM SERVER: " + modifiedSentence);
            }
            System.out.println("Closing client" );
            clientSocket.close();
        } catch (Exception e) {
            
        }

    }
    
}
