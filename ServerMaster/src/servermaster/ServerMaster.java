/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package servermaster;

import java.net.*;
import java.io.*;

/**
 *
 * @author laurencewong
 */
public class ServerMaster {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
         String clientSentence;
         String capitalizedSentence;
         try {
         ServerSocket welcomeSocket = new ServerSocket(6789);

         while(true)
         {
            Socket connectionSocket = welcomeSocket.accept();
            BufferedReader inFromClient =
               new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
            clientSentence = inFromClient.readLine();
            System.out.println("Received: " + clientSentence);
            capitalizedSentence = clientSentence.toUpperCase() + '\n';
            outToClient.writeBytes(capitalizedSentence);
         }
         } catch (Exception e)
         {
         }
    }
    
}
