/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.chunkserver;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import tfs.util.Message;
import tfs.util.MySocket;

//MAX_CHUNK_SIZE = 67108864;
/**
 *
 * @author laurencewong
 */
public class ChunkServer {

    final int MAX_CHUNK_SIZE = 67108864; //64MB in bytes

    //have chunks named: filename<chunk#>
    //for example: if the actual file is stories.txt and it has 3 chunks,
    //the chunks would be named stories-0, stories-1, stories-3
    private class Chunk {

        String mChunkFileName;
        File mChunkFile;
        int mCurrentSize;

        public Chunk(String inName) throws IOException {
            inName = inName.replaceAll("/", ".");
            mChunkFile = new File(inName);
            mChunkFileName = inName;
            if (!mChunkFile.createNewFile()) {
                //createNewFile returns false if file already exists
                //not sure how to handle this yet
            } else {
                //file did not exist and has been created
            }
        }

        public boolean AppendTo(byte[] inData) {
            try {
                Path filePath = Paths.get(mChunkFileName);
                File f = new File(mChunkFileName);
                if (f.exists()) {
                    BufferedInputStream in = new BufferedInputStream(Files.newInputStream(filePath));
                    int skippedBytes = 0;
                    byte[] intByte = new byte[4];
                    in.mark(0);
                    while (in.read() != -1) {
                        in.reset(); //move back to the point before the byte
                        in.read(intByte);
                        skippedBytes += 4;
                        int bytesToSkip = ByteBuffer.wrap(intByte).getInt();
                        in.skip(bytesToSkip);
                        skippedBytes += bytesToSkip;
                        in.mark(skippedBytes);//mark the position before the next int
                    }
                    System.out.println("Skipped: " + skippedBytes + "bytes");
                } else {
                    throw new IOException("File does not exist");
                }
                OutputStream out = new BufferedOutputStream(Files.newOutputStream(filePath, CREATE, APPEND));
                out.write(ByteBuffer.allocate(4).putInt(inData.length).array());
                out.write(inData);
                out.close();
                mCurrentSize += 4 + inData.length; //4 is for the int
                System.out.println("Finished writing file");
            } catch (IOException ioe) {
                System.out.println("Unable to complete writing to file");
                System.out.println(ioe.getMessage());
                return false;
            }
            return true;
        }

        //assumes that outData has already been instantiated to the correct size
        public boolean ReadFrom(int offset, int length, byte[] outData) {
            try {
                Path filePath = Paths.get(mChunkFileName);
                BufferedInputStream br = new BufferedInputStream(Files.newInputStream(filePath, READ));

                if (br.read(outData, offset, length) > 0) {
                } else {
                    System.out.println("Failed to read given length from offset");
                }
            } catch (IOException ie) {
                System.out.println("Problem reading file");
                System.out.println(ie.getMessage());
                return false;
            }
            return true;
        }

        //automatically fills up the chunk to the max chunk size
        public boolean FillUp() {
            mCurrentSize = MAX_CHUNK_SIZE;
            return true;
        }
    }

    private abstract class PrimaryTimer extends TimerTask {

        public String chunkFilename = null;

        public PrimaryTimer(String filename) {
            chunkFilename = filename;
        }
    }

    private class ChunkAppendRequest {

        boolean mIsPrimary; //whether or not I am the primary of this
        ArrayList<String> mServers; //ID's of other servers servicing this request
        //mServers will only have the primary ID if I am not the primary
        int mSuccessfulServers; //number of servers that have replied with success
        int mTotalResponses;
        String mChunkName;
        MySocket mClientSocket; //Reference to the socket of the client that wants
        //the data

        public ChunkAppendRequest(boolean isPrimaryServer, String inChunkName) {
            mIsPrimary = isPrimaryServer;
            mChunkName = inChunkName;
            mServers = new ArrayList<>();
        }
    }

    ServerSocket mListenSocket;
    HashMap<String, Chunk> mChunks;
    MySocket mServerSocket;
    ArrayList<MySocket> mClients;
    ArrayList<MySocket> mChunkServers;
    ArrayDeque<Message> mPendingMessages;
    ArrayList<Timer> mPrimaryTimers;
    HashMap<String, ChunkAppendRequest> mChunkServices;

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
        mChunkServers = new ArrayList<MySocket>();
        mPendingMessages = new ArrayDeque<>();
        mPrimaryTimers = new ArrayList<Timer>();
        mChunkServices = new HashMap<String, ChunkAppendRequest>();
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
                        case "Chunk":
                            synchronized (mChunkServers) {
                                mChunkServers.add(newConnection);
                                System.out.println("Adding new chunkserver");
                            }
                            break;
                        case "ServerMaster":
                            synchronized (mServerSocket) {
                                mServerSocket = newConnection;
                                System.out.println("Setting new server master");
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
                    synchronized (mClients) {
                        for (MySocket clientSocket : mMain.mClients) {

                            if (clientSocket.hasData()) {
                                Message messageReceived = new Message(clientSocket.ReadBytes());
                                Message messageSending = ParseClientInput(messageReceived);
                                messageSending.SetSocket(clientSocket);
                                mPendingMessages.push(messageSending);
                            }
                        }
                    }
                    synchronized (mChunkServers) {
                        for (MySocket chunkServerSocket : mMain.mChunkServers) {
                            if (chunkServerSocket.hasData()) {
                                Message messageReceived = new Message(chunkServerSocket.ReadBytes());
                                Message messageSending = ParseChunkInput(messageReceived);
                                messageSending.SetSocket(chunkServerSocket);
                                mPendingMessages.push(messageSending);
                            }
                        }
                    }
                    synchronized (mServerSocket) {
                        if (mServerSocket.hasData()) {
                            Message messageReceived = new Message(mServerSocket.ReadBytes());
                            Message messageSending = ParseServerInput(messageReceived);
                            messageSending.SetSocket(mServerSocket);
                            mPendingMessages.push(messageSending);
                        }
                    }
                    while (!mPendingMessages.isEmpty()) {
                        mPendingMessages.pop().Send();
                    }
                    ChunkServerClientThread.sleep(100);

                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());

                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
        }

        public Message ParseServerInput(Message m) {
            Message outputToServer = new Message();
            System.out.println("Parsing server input");
            String input = m.ReadString();
            switch (input.toLowerCase()) {
                case "print":
                    System.out.println(m.ReadString());
                    break;
                case "sm-makeprimary":
                    BecomePrimary(m.ReadString(), outputToServer);
                    break;
            }
            return outputToServer;
        }

        public void UnBecomePrimary(String chunkFilename) {
            synchronized (mChunkServices) {
                if (mChunkServices.get(chunkFilename) == null) {
                    //chunk does not exist
                    System.out.println("SUPER BAD ERROR: I am scheduled to unbecome"
                            + " the primary of a missing chunk! :( :( :(");
                }
                mChunkServices.get(chunkFilename).mIsPrimary = false;
                //do other things that happen when i am no longer primary
            }
        }

        public void BecomePrimary(String chunkFilename, Message output) {
            //check if chunkfile exists
            if (mChunks.get(chunkFilename) == null) {
                //chunk does not exist
                //not sure what to do here :(
                output.WriteDebugStatement("Tried to become primary of chunk that does not exist");
                return;
            }
            //chunk does exist
            ChunkAppendRequest chunkService = mChunkServices.get(chunkFilename);
            if (chunkService == null) {
                mChunkServices.put(chunkFilename, new ChunkAppendRequest(true, chunkFilename));
            } else {
                chunkService.mIsPrimary = true;
            }
            //scheduled to stop being primary in 60s
            Timer newTimer = new Timer();
            newTimer.schedule(new PrimaryTimer(chunkFilename) {
                @Override
                public void run() {
                    UnBecomePrimary(chunkFilename);
                }
            }, 0, 60000);
            mPrimaryTimers.add(newTimer);
            output.WriteDebugStatement("Am the primary of chunk " + chunkFilename);
        }

        public Message ParseChunkInput(Message m) {
            Message outputToChunk = new Message();
            String input = m.ReadString();
            System.out.println("Parsing chunk input");
            switch (input.toLowerCase()) {
                case "seekfile":
                case "seek":
                    System.out.println("Reading file");
                    break;
                case "appendfile":
                case "append":
                    System.out.println("Writing file");
                    break;
                case "cs-appendfileresponse":
                    //filename, serverinfo, success
                    EvaluateCSAppendFileResponse(m);
                    break;
                case "print":
                    System.out.println(m.ReadString());
                    break;
                default:
                    System.out.println("chunk gave chunk server wrong command");
                    break;
            }
            System.out.println("Finished chunk input");
            return outputToChunk;
        }

        public void EvaluateCSAppendFileResponse(Message m) {
            String filename = m.ReadString();
            String serverInfo = m.ReadString();
            String success = m.ReadString();
            ChunkAppendRequest chunkService = mChunkServices.get(filename);
            for (String server : chunkService.mServers) {
                if (server.compareTo(serverInfo) == 0) {
                    chunkService.mTotalResponses++;
                    if (success.compareTo("successful") == 0) {
                        chunkService.mSuccessfulServers++;
                    }
                    break;
                }
            }
            //need to check if the server is not in the chunkservices (which should never ever happen)
        }

        public Message ParseClientInput(Message m) {
            Message outputToClient = new Message();
            String input = m.ReadString();
            System.out.println("Parsing client input");
            switch (input.toLowerCase()) {
                case "print":
                    System.out.println(m.ReadString());
                    break;
                case "seekfile":
                case "seek":
                    SeekFile(m.ReadString(), m.ReadInt(), m.ReadInt(), outputToClient);
                    break;
                case "appendfile":
                case "append":
                    System.out.println("Writing file");
                    break;
                default:
                    System.out.println("Client gave chunk server wrong command");
                    break;
            }
            System.out.println("Finished client input");
            return outputToClient;
        }

        public void SeekFile(String filename, int offset, int length, Message output) {
            filename = filename.replaceAll("/", ".");
            byte[] data = new byte[length];
            Chunk chunkToRead = mChunks.get(filename);
            if (chunkToRead == null) {
                output.WriteDebugStatement("Chunk does not exist");
                System.out.println("Chunk does not exist");
            } else {
                if (chunkToRead.ReadFrom(offset, length, data)) {
                    output.WriteString("cs-seekfileresponse");
                    output.WriteInt(length);
                    output.AppendData(data);
                } else {
                    output.WriteDebugStatement("Failed to read given length from offset");
                }
            }
        }

        public void AppendToFile(Message input, Message output) {
            //Message format: filename, data, replicaip:port, ... 
            String filename = input.ReadString();
            byte[] inData = input.ReadData(input.ReadInt());
            int numReplicas = input.ReadInt();

            filename = filename.replace("/", ".");

            //try to find a chunkservicerequest with this filename.  server should have
            //already sent a request to this server since this is the primary
            ChunkAppendRequest mChunkService = mChunkServices.get(filename);
            if (mChunkService == null) {
                //I should be the primary, but I don't have the service request yet
                //make a new one and assume that the server will tell me i'm primary later
                System.out.println("Making a a new chunkservicerequest and becoming primary"
                        + " even though server hasn't told me yet.");
                mChunkService = new ChunkAppendRequest(true, filename);
                mChunkServices.put(filename, mChunkService);
            }
            for (int i = 0; i < numReplicas; ++i) {
                mChunkService.mServers.add(input.ReadString());
            }
            Chunk chunkToAppend = mChunks.get(filename);
            if (chunkToAppend.mCurrentSize + 4 + inData.length > MAX_CHUNK_SIZE) {
                //make a new chunk
                //tell server that you're making a new chunk
                //fill up the current chunk
                chunkToAppend.FillUp();
                //write into the new chunk
            } else {
                //there is enough space
                if (chunkToAppend.AppendTo(inData)) {
                    //it was successful
                } else {
                    //it was not successful and need to tell primary unless
                    //i am the primary
                }
            }
        }

    }
}
