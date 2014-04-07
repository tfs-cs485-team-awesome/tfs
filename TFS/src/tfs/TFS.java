/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs;

import java.io.*;
import java.net.*;
import servermaster.ServerMaster;
import client.Client;

/**
 *
 * @author laurencewong
 */
public class TFS {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Needs a command line parameter");
        } else {
            switch (args[0]) {
                case "Server":
                case "server":
                    ServerMaster server;
                    if (args.length == 2) {
                        server = new ServerMaster(Integer.valueOf(args[1]));
                    } else {
                        server = new ServerMaster();
                    }

                    server.RunLoop();
                    break;
                case "Client":
                case "client":
                    Client client = new Client("localhost:blah");
                    client.RunLoop();
                    break;
                default:
                    System.out.println("Invalid argument");
                    break;
            }
        }
    }
}
