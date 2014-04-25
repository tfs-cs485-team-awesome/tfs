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
import tfs.util.ChunkPendingAppend;
import tfs.util.HeartbeatSocket;
import tfs.util.Callbackable;

//MAX_CHUNK_SIZE = 67108864;
/**
 *
 * @author laurencewong
 */
public class ChunkServer implements Callbackable {

    final int MAX_CHUNK_SIZE = 67108864; //64MB in bytes

    @Override
    public void Callback(String inParameter) {
        synchronized (mSocketsToClose) {
            mSocketsToClose.push(inParameter);
        }
    }

    //have chunks named: filename<chunk#>
    //for example: if the actual file is stories.txt and it has 3 chunks,
    //the chunks would be named stories-0, stories-1, stories-3
    private class Chunk {

        String mChunkFileName;
        File mChunkFile;
        int mCurrentSize;

        public Chunk(String inName) throws IOException {
            inName = inName.replaceAll("/", ".");
            System.out.println("inName = " + inName);
            mChunkFile = new File(inName);
            System.out.println("inName absolute dir: " + mChunkFile.getAbsolutePath());
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
                    in.close();
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

    String[] mServerInfo;
    String mListenPort;
    String mID;
    ServerSocket mListenSocket;
    HashMap<String, Chunk> mChunks;
    MySocket mServerSocket;
    ArrayList<MySocket> mClients;
    ArrayList<MySocket> mChunkServers;
    ArrayDeque<Message> mPendingMessages;
    ArrayList<Timer> mPrimaryTimers;
    ArrayDeque<String> mSocketsToClose;
    HashMap<String, ChunkPendingAppend> mChunkServices;
    HeartbeatSocket mHeartbeatSocket;

    public final String GetIP() throws IOException {
        String ip = InetAddress.getLocalHost().toString();
        ip = ip.substring(ip.indexOf("/") + 1);
        return ip;
    }

    public void InitConnectionWithMasterServer(MySocket inSocket) {
        try {
            System.out.println("Telling server that I am a chunkserver");
            Message toServer = new Message();
            toServer.WriteString("ChunkServer");
            toServer.WriteInt(mListenSocket.getLocalPort());
            inSocket.WriteMessage(toServer);
            //wait for response back with ID
            while (!inSocket.hasData()) {
            }
            Message fromServer = new Message(inSocket.ReadBytes());
            if (!fromServer.ReadString().equalsIgnoreCase("setid")) {
                System.out.println("Did not get setid from server.  Shutting down");
                System.exit(1);
            } else {
                mID = fromServer.ReadString();
                mHeartbeatSocket = new HeartbeatSocket(mID, inSocket.GetID(), this);
                mHeartbeatSocket.start();
                System.out.println("Setting id to " + mID);
            }

        } catch (IOException ioExcept) {
            System.out.println("Problem initializing connection with server");
            System.out.println(ioExcept.getMessage());
        }
    }

    public void InitConnectionWithChunkServer(MySocket inSocket) {
        try {
            System.out.println("Telling chunkserver that I am a fellow chunkserver");
            Message toServer = new Message();
            toServer.WriteString("ChunkServer");
            toServer.WriteString(mID);
            toServer.WriteString(mHeartbeatSocket.GetIPAndPort());
            inSocket.WriteMessage(toServer);

        } catch (IOException ioe) {
            System.out.println("Problem initializing connection with server");
            System.out.println(ioe.getMessage());
        }
    }

    public void Init() {
        mChunks = new HashMap<String, Chunk>();
        mClients = new ArrayList<MySocket>();
        mChunkServers = new ArrayList<MySocket>();
        mPendingMessages = new ArrayDeque<>();
        mPrimaryTimers = new ArrayList<Timer>();
        mChunkServices = new HashMap<String, ChunkPendingAppend>();
        mSocketsToClose = new ArrayDeque<>();
    }

    /**
     *
     * @param inConfigurationInformation Needs to be in format: "serverIP:port"
     *
     */
    public ChunkServer(String inConfigurationInformation, String inPort) {
        Init();
        mServerInfo = inConfigurationInformation.split(":");
        mListenPort = inPort;
        if (mServerInfo.length != 2) {
            System.out.println("Server information " + mServerInfo + " is in wrong format");
            return;
        }
        try {
            mListenSocket = new ServerSocket(Integer.valueOf(mListenPort));
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
        ConnectToServerMaster();
    }

    public void ConnectToServerMaster() {
        while (true) {
            try {
                mServerSocket = new MySocket(mServerInfo[0], Integer.valueOf(mServerInfo[1]));
                break;
            } catch (IOException e) {
                if (e.getMessage().contentEquals("Connection refused")) {
                    System.out.println("Master Server is not online.  Attempting to reconnect in 3s");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e1) {
                        System.out.println("Exception sleeping");
                        System.out.println(e1.getMessage());
                    }
                }
            }
        }
        InitConnectionWithMasterServer(mServerSocket);
    }

    public MySocket GetSocketForID(String inSocketID) {
        for (MySocket s : mChunkServers) {
            if (s.GetID().equalsIgnoreCase(inSocketID)) {
                return s;
            }
        }
        for (MySocket s : mClients) {
            if (s.GetID().equalsIgnoreCase(inSocketID)) {
                return s;
            }
        }
        if (inSocketID.equalsIgnoreCase("servermaster")) {
            System.out.println("Removing server master");
            return mServerSocket;
        }
        return null;
    }

    public void RemoveSocket(MySocket inSocket) {
        if (mChunkServers.remove(inSocket)) {
            return;
        }
        if (mClients.remove(inSocket)) {
            return;
        }
        if (inSocket.GetID().equalsIgnoreCase(mServerSocket.GetID())) {
            System.out.println("Closing server master");
            mServerSocket = null;
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
                                newConnection.SetID(m.ReadString());
                                mHeartbeatSocket.AddHeartbeat(newConnection.GetID(), m.ReadString());
                                System.out.println("Adding new client with ID: " + newConnection.GetID());
                            }
                            break;
                        case "ChunkServer":
                            synchronized (mChunkServers) {
                                String id = m.ReadString();
                                newConnection.SetID(id);
                                mChunkServers.add(newConnection);
                                mHeartbeatSocket.AddHeartbeat(newConnection.GetID(), m.ReadString());
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
            LoadFileStructure();
            while (true) {
                try {
                    synchronized (mClients) {
                        for (MySocket clientSocket : mClients) {

                            if (clientSocket.hasData()) {
                                Message messageReceived = new Message(clientSocket.ReadBytes());
                                Message messageSending = ParseClientInput(messageReceived);
                                messageSending.SetSocket(clientSocket);
                                mPendingMessages.push(messageSending);
                            }
                        }
                    }
                    synchronized (mChunkServers) {
                        for (MySocket chunkServerSocket : mChunkServers) {
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
                    synchronized (mSocketsToClose) {
                        while (!mSocketsToClose.isEmpty()) {
                            String socketID = mSocketsToClose.pop();
                            MySocket socketToClose = GetSocketForID(socketID);
                            if (socketToClose == null) {
                                System.out.println("Cannot find socket: " + socketID);
                                continue;
                            }
                            System.out.println("Removing socket " + socketID);
                            socketToClose.close();
                            RemoveSocket(socketToClose);
                            //need to remove socket
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

        public Message ParseServerInput(Message m) {
            Message outputToServer = new Message();
            System.out.println("Parsing server input");
            String input = m.ReadString();
            switch (input.toLowerCase()) {
                case "print":
                    System.out.println(m.ReadString());
                    break;
                case "sm-makeprimary":
                    System.out.println("Was told to become primary by server master");
                    //BecomePrimary(m.ReadString(), outputToServer);
                    break;
                case "sm-makenewfile":
                    MakeNewChunk(m.ReadString());
                    SaveFileStructure();
                    break;

            }
            return outputToServer;
        }
        /*
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
         */
        /*
         public void BecomePrimary(String chunkFilename, Message output) {
         //check if chunkfile exists
         if (mChunks.get(chunkFilename) == null) {
         //chunk does not exist
         //not sure what to do here :(
         System.out.println("Tried to become primary of chunk that does not exist");
         output.WriteDebugStatement("Tried to become primary of chunk that does not exist");
         return;
         }
         //chunk does exist
         System.out.println("Chunk I am to become primary of exists");
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
         */

        public void MakeNewChunk(String chunkFilename) {
            try {
                if (mChunks.get(chunkFilename) == null) {
                    if (chunkFilename.startsWith("/")) {
                        chunkFilename = chunkFilename.substring(1, chunkFilename.length());
                    }
                    chunkFilename = chunkFilename.replaceAll("/", ".");
                    System.out.println("Making new chunk: " + chunkFilename);
                    mChunks.put(chunkFilename, new Chunk(chunkFilename));
                } else {
                    System.out.println("Chunk already exists in mChunks wtf");
                }
            } catch (IOException ioe) {
                System.out.println("Unable to make new chunk from server");
                System.out.println(ioe.getMessage());
            }
        }

        public void LoadFileStructure() {
            File file = new File("SYSTEM_LOG.txt");
            // create file if it does not exist
            if (!file.exists()) {
                try {
                    file.createNewFile();
                    return;
                } catch (IOException ie) {
                    System.out.println("Unable to create TFS structure file");
                }
            }

            // create an input stream to read the data from the file
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line;
                while ((line = br.readLine()) != null) {
                    MakeNewChunk(line);
                }
                br.close();
                
                //tell server what chunks I have
                System.out.println("Telling server what chunks i have");
                Message chunksToServer = new Message();
                chunksToServer.WriteString("chunksihave");
                chunksToServer.WriteString(mID);
                chunksToServer.WriteInt(mChunks.values().size());
                for(Chunk c : mChunks.values()) {
                    String modifiedChunkFilename = c.mChunkFileName.replaceAll("\\.", "/");
                    chunksToServer.WriteString(modifiedChunkFilename);
                }
                chunksToServer.SetSocket(mServerSocket);
                mPendingMessages.push(chunksToServer);
            } catch (IOException ie) {
                System.out.println("Unable to read TFS structure file");
            }
        }

        public void SaveFileStructure() {
            System.out.println("Saving file structure");
            try {
                PrintWriter pw = new PrintWriter(new FileWriter("SYSTEM_LOG.txt", false));
                for (Chunk c : mChunks.values()) {
                    pw.write(c.mChunkFileName + "\r\n");
                }
                pw.close();
            } catch (IOException ie) {
                System.out.println("Unable to write to TFS structure file");
            }
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
                case "cs-appendreplica":
                    //filename, size, data
                    EvaluateCSAppendFile(m, outputToChunk);
                    SaveFileStructure();
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

        public void EvaluateCSAppendFile(Message m, Message output) {
            System.out.println("Got append request from another chunk server");
            String filename = m.ReadString();
            int dataSize = m.ReadInt();
            byte[] data = m.ReadData(dataSize);
            System.out.println("For file: " + filename);
            output.WriteString("cs-appendfileresponse");
            output.WriteString(filename);
            output.WriteString(mID);
            output.WriteInt(AppendToFile(filename, data) ? 1 : 0);
        }

        public void EvaluateCSAppendFileResponse(Message m) {
            System.out.println("Received append file response");
            String filename = m.ReadString();
            String serverInfo = m.ReadString();
            int success = m.ReadInt();
            ChunkPendingAppend chunkService = mChunkServices.get(filename);
            chunkService.UpdateServerStatus(serverInfo, (success == 1), filename);
            if (chunkService.AllServersReplied()) {
                System.out.println("Chunk server finished append");
                if (chunkService.WasSuccessful() == false) {
                    System.out.println("Not all servers were successful");
                    //chunkService.resendMessage();
                } else {
                    System.out.println("All servers were successful");
                    //send success to client
                    mChunkServices.remove(filename);
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
                    ClientAppendToFile(m);
                    SaveFileStructure();
                    break;
                case "readfile":
                    ReadFile(m, outputToClient);
                    break;
                default:
                    System.out.println("Client gave chunk server wrong command");
                    break;
            }
            System.out.println("Finished client input");
            return outputToClient;
        }

        public void ReadFile(Message input, Message output) {
            //Message format: filename, data, replicaip:port, ... 
            String fileName = input.ReadString();
            int numReplicas = input.ReadInt();
            String[] replicaInfo = new String[numReplicas];
            for (String s : replicaInfo) {
                s = input.ReadString();
            }
            try {
                fileName = fileName.replaceAll("/", ".");
                Path filePath = Paths.get(fileName);
                System.out.println("Reading file at " + filePath.toString());
                byte[] data = Files.readAllBytes(filePath);
                output.WriteString("cs-readfileresponse");
                output.WriteString(fileName);
                output.WriteInt(data.length);
                output.AppendData(data);
            } catch (IOException ioe) {
                System.out.println("Problem reading from file");
                System.out.println(ioe.getMessage());
            }
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

        public boolean AppendToFile(String filename, byte[] inData) {
            Chunk chunkToAppend = mChunks.get(filename);
            if (chunkToAppend == null) {
                MakeNewChunk(filename);
                chunkToAppend = mChunks.get(filename);
            }
            if (chunkToAppend.mCurrentSize + 4 + inData.length > MAX_CHUNK_SIZE) {
                System.out.println("Not enough space to append data");
                //make a new chunk
                //tell server that you're making a new chunk
                //fill up the current chunk
                chunkToAppend.FillUp();
                //write into the new chunk
            } else {
                //there is enough space
                if (chunkToAppend.AppendTo(inData)) {
                    System.out.println("Appending data was successful");
                    return true;
                    //it was successful
                } else {
                    System.out.println("Appending data was not successful");
                    //it was not successful and need to tell primary unless
                    //i am the primary
                    return false;
                }
            }
            return false;
        }

        public void ClientAppendToFile(Message input) {
            //Message format: clientinfo, filename, data, replicaip:port, ... 
            String filename = input.ReadString();
            int numReplicas = input.ReadInt();
            String[] replicaInfo = new String[numReplicas];
            for (int i = 0; i < replicaInfo.length; ++i) {
                replicaInfo[i] = input.ReadString();
            }
            int dataSize = input.ReadInt();
            byte[] inData = input.ReadData(dataSize);

            filename = filename.replace("/", ".");

            //try to find a chunkservicerequest with this filename.  server should have
            //already sent a request to this server since this is the primary
            ChunkPendingAppend mChunkService = mChunkServices.get(filename);
            if (mChunkService == null) {
                //I should be the primary, but I don't have the service request yet
                //make a new one and assume that the server will tell me i'm primary later
                System.out.println("Making a a new chunkservicerequest.");
                mChunkService = new ChunkPendingAppend(filename);
                mChunkServices.put(filename, mChunkService);
            } else {
                System.out.println("Using old chunkpendingappend.  shouldn't happen.  might happen with multiple clients");
            }
            for (String s : replicaInfo) {
                System.out.println(s);
                try {
                    MySocket newChunkServerSocket = new MySocket(s);
                    InitConnectionWithChunkServer(newChunkServerSocket);
                    mChunkServers.add(newChunkServerSocket);
                    mChunkService.AddServer(newChunkServerSocket);
                } catch (IOException ioe) {
                    System.out.println(ioe.getMessage());

                }
            }
            AppendToFile(filename, inData);
            mChunkService.GetMessage().WriteString("cs-appendreplica");
            mChunkService.GetMessage().WriteString(filename);
            mChunkService.GetMessage().WriteInt(inData.length);
            mChunkService.GetMessage().AppendData(inData);
            try {
                mChunkService.SendRequest();
            } catch (IOException ioe) {
                System.out.println("Had problem writing data to socket");
            }
        }

    }
}
