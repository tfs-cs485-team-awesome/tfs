/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import tfs.src.chunkserver.ChunkServer;
import tfs.src.client.Client;
import tfs.src.servermaster.ServerMaster;
import tfs.src.test.TestCases;

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
            File f = new File("config.cfg");
            if (f.exists()) {
                try {
                    ArrayList<String> readInArgs = new ArrayList<>();
                    BufferedReader inFromConfig = new BufferedReader(new FileReader(f));
                    String readIn = inFromConfig.readLine();
                    while(readIn != null) {
                        readInArgs.add(readIn);
                        readIn = inFromConfig.readLine();
                    }
                    System.out.println("Successfully read config.cfg");
                    String[] inargs = new String[readInArgs.size()];
                    inargs = readInArgs.toArray(inargs);
                    start(inargs);
                } catch (FileNotFoundException fnfe) {
                    System.out.println("Config file not found");
                    System.out.println(fnfe.getMessage());
                } catch (IOException ioe) {
                    System.out.println("Problem reading file");
                    System.out.println(ioe.getMessage());
                }
            } else {
                System.out.println("Needs a command line parameter or config file");
            }
        } else {
            start(args);
        }
    }

    public static void start(String[] args) {
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
            case "test":
            case "TEST":
            case "Test":
                TestCases.main(args);
            default:
                System.out.println("Invalid type");
                break;

        }
    }
}
