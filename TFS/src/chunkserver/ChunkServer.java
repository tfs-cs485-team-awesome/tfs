/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chunkserver;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.ArrayList;
import tfs.Message;
import tfs.MySocket;

/**
 *
 * @author laurencewong
 */
public class ChunkServer {

    private class Chunk {

        boolean mPrimaryChunkHolder;
        File mChunkFile;
        RandomAccessFile mChunkRandomFile;

        public Chunk(int inName) {

            mPrimaryChunkHolder = false; //default to not primary chunk holder
            mChunkFile = new File(String.valueOf(inName));
            try {
                mChunkRandomFile = new RandomAccessFile(mChunkFile, "rw"); //makes the new file
                mChunkRandomFile.setLength(67108864); //magic number - 64MB
            } catch (FileNotFoundException fe) {
                System.out.println("Not able to create randomAccessFile for chunk");
                System.out.println(fe.getMessage());
            } catch (IOException ioe) {
                System.out.println("Not able to extend randomChunkFile to 64MB");
            }
        }

    }

    ServerSocket mListenSocket;
    HashMap<String, Chunk> mChunks;
    MySocket mServerSocket;
    ArrayList<MySocket> mClients;
    int mCurChunkName;

    public void InitConnectionWithServer(MySocket inSocket) {
        try {
            System.out.println("Telling server that I am a chunkserver");
            Message toServer = new Message();
            toServer.WriteString("ChunkServer");
            inSocket.WriteMessage(toServer);
            
        } catch (IOException ioExcept) {
            System.out.println("Problem initializing connection with server");
            System.out.println(ioExcept.getMessage());
        }
    }

    public void Init() {
        mChunks = new HashMap<String, Chunk>();
        mClients = new ArrayList<MySocket>();
    }

    /**
     *
     * @param inConfigurationInformation Needs to be in format: "serverIP:port"
     *
     */
    public ChunkServer(String inConfigurationInformation, String inPort) {
        Init();
        String[] serverInformation = inConfigurationInformation.split(":");
        if (serverInformation.length != 2) {
            System.out.println("Server information " + serverInformation + " is in wrong format");
            return;
        }
        try {
            mListenSocket = new ServerSocket(Integer.valueOf(inPort));
            mServerSocket = new MySocket(serverInformation[0], Integer.valueOf(serverInformation[1]));
            
            InitConnectionWithServer(mServerSocket);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    public Chunk MakeNewChunk() {
        return new Chunk(mCurChunkName++);
    }

    public void RunLoop() {
        while (true) {
            try {
                ChunkServerClientThread clientServingThread = new ChunkServerClientThread(this);
                clientServingThread.start();

                while (true) {
                    MySocket newConnection = new MySocket(mListenSocket.accept());
                    Message m = new Message(newConnection.ReadBytes());
                    String connectionType = m.ReadString();
                    switch (connectionType) {
                        case "Client":
                            synchronized (mClients) {
                                mClients.add(newConnection);
                                System.out.println("Adding new client");
                            }
                            break;
                        default:
                            System.out.println("Server was told new connection of type: " + connectionType);
                            break;
                    }

                }

            } catch (Exception e) {

            }
        }
    }

    private class ChunkServerClientThread extends Thread {

        ChunkServer mMain;
        int mClientNum;

        public ChunkServerClientThread(ChunkServer inMain) {
            mMain = inMain;
            System.out.println("Creating chunkServer client serving thread");

        }

        public void run() {
            while (true) {
                try {
                    //Read/Write from server

                    //Read/Write from clients
                    synchronized (mClients) {
                        for (MySocket clientSocket : mMain.mClients) {
                            
                            if (clientSocket.hasData()) {
                                Message fromClient = new Message(clientSocket.ReadBytes());
                                ParseClientInput(fromClient);
                            }
                        }
                    }
                    
                    ChunkServerClientThread.sleep(100);

                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());

                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
        }

        public void ParseServerInput(String input) {
            System.out.println("Parsing server input");
            switch (input) {
                case "NewChunk":
                    System.out.println("Making new chunk");

                    break;
            }
        }

        public void ParseClientInput(Message m) {
            String input = m.ReadString();
            System.out.println("Parsing client input");
            switch (input) {
                case "ReadFile":
                    System.out.println("Reading file");
                    break;
                case "WriteFile":
                    System.out.println("Writing file");
                    break;
                default:
                    System.out.println("Client gave chunk server wrong command");
                    break;
            }
            System.out.println("Finished client input");
            return;
        }

    }
}
