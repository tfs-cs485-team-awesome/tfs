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
    }
    /**
     * @param args the command line arguments
     */
    public void RunLoop() {
        String sentence;
        String modifiedSentence;
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        Socket clientSocket;
        System.out.println("Starting client");
        try {
            clientSocket = new Socket("localhost", 6789);
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
