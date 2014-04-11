/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs;

import java.io.*;
import java.net.*;
import tfs.src.servermaster.ServerMaster;
import tfs.src.client.Client;
import tfs.src.chunkserver.ChunkServer;

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
                    if (args.length == 2) {
                        Client client = new Client(args[1]);
                        client.RunLoop();
                    } else {
                        System.out.println("Need to specify address and port for client");
                        return;
                    }
                    break;
                case "Chunk":
                case "ChunkServer":
                case "chunk":
                case "chunkserver":
                    if (args.length == 3) {
                        ChunkServer chunk = new ChunkServer(args[1], args[2]);
                        chunk.RunLoop();
                    }
                    break;
                default:
                    System.out.println("Invalid argument");
                    break;

            }
        }
    }
}
