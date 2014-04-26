/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.chunkserver;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
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
        long mLastModified;

        public Chunk(String inName) throws IOException {
            this(inName, System.currentTimeMillis() / 1000);
        }

        public Chunk(String inName, long timeStamp) throws IOException {
            inName = inName.replaceAll("/", ".");
            System.out.println("inName = " + inName);
            mChunkFile = new File(inName);
            System.out.println("inName absolute dir: " + mChunkFile.getAbsolutePath());
            mChunkFileName = inName;
            mLastModified = timeStamp;
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
                RandomAccessFile raf = new RandomAccessFile(f, "rw");

                if (f.exists()) {
                    //BufferedInputStream in = new BufferedInputStream(Files.newInputStream(filePath));
                    //byte[] intByte = new byte[4];
                    //in.mark(0);
                    int skippedBytes = 0;
                    while (raf.getFilePointer() < raf.length()) {
                        //in.reset(); //move back to the point before the byte
                        //in.read(intByte);
                        int subFilesize = raf.readInt();
                        raf.skipBytes(subFilesize);
                        skippedBytes += subFilesize;

                    }
                    System.out.println("Skipped: " + skippedBytes + "bytes");
                } else {
                    throw new IOException("File does not exist");
                }
                raf.close();
                OutputStream out = new BufferedOutputStream(Files.newOutputStream(filePath, CREATE, APPEND));
                out.write(ByteBuffer.allocate(4).putInt(inData.length).array());
                out.write(inData);
                out.close();
                mCurrentSize += 4 + inData.length; //4 is for the int
                System.out.println("Finished writing file");
                // update time stamp
                mLastModified = System.currentTimeMillis() / 1000;
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
                br.close();
            } catch (IOException ie) {
                System.out.println("Problem reading file");
                System.out.println(ie.getMessage());
                return false;
            }
            return true;
        }

        public byte[] ReadAllBytes() {
            try {
                Path filePath = Paths.get(mChunkFileName);
                return Files.readAllBytes(filePath);
            } catch (IOException ioe) {
                System.out.println("Problem reading from file");
                System.out.println(ioe.getMessage());
            }
            return null;
        }

        public boolean Overwrite(byte[] inData) {
            try {
                Path filePath = Paths.get(mChunkFileName);
                Files.write(filePath, inData, WRITE);
                return true;
            } catch (IOException ioe) {
                System.out.println("Can't overwrite file: " + mChunkFileName);
            }
            return false;
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
    HashMap<String, MySocket> mChunkServers;
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
        mChunkServers = new HashMap<String, MySocket>();
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
        if (mChunkServers.containsKey(inSocketID)) {
            return mChunkServers.get(inSocketID);
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
        if (mChunkServers.remove(inSocket.GetID()) != null) {
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
                                mChunkServers.put(id, newConnection);
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
                        for (MySocket chunkServerSocket : mChunkServers.values()) {
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

        public Message ParseServerInput(Message m) throws IOException {
            Message outputToServer = new Message();
            String input = m.ReadString();
            System.out.println("Parsing server input " + input);
            switch (input.toLowerCase()) {
                case "print":
                    System.out.println(m.ReadString());
                    break;
                case "sm-makeprimary":
                    System.out.println("Was told to become primary by server master");
                    //BecomePrimary(m.ReadString(), outputToServer);
                    break;
                case "sm-makenewfile":
                    //TODO handle the fact that this almost always comes after the client request
                    MakeNewChunk(m.ReadString());
                    SaveFileStructure();
                    break;
                case "sm-deletefile":
                    DeleteFile(m.ReadString());
                    SaveFileStructure();
                    break;
                case "sm-outofdatechunk":
                    UpdateChunk(m.ReadString(), m.ReadString());
                    break;
            }
            return outputToServer;
        }

        public void UpdateChunk(String filename, String primaryChunkInfo) throws IOException {
            if (!mChunks.containsKey(filename)) {
                System.out.println("Trying to update chunk that does not exist on this server: " + filename);
                return;
            }
            MySocket newChunkServerSocket;

            synchronized (mMain.mChunkServers) {
                if (!mMain.mChunkServers.containsKey(primaryChunkInfo)) {
                    newChunkServerSocket = new MySocket(primaryChunkInfo);
                    InitConnectionWithChunkServer(newChunkServerSocket);
                    mChunkServers.put(primaryChunkInfo, newChunkServerSocket);
                } else {
                    newChunkServerSocket = mMain.mChunkServers.get(primaryChunkInfo);
                }
            }
            Message outputToChunk = new Message();
            outputToChunk.WriteString("cs-updatefile");
            outputToChunk.WriteString(filename);
            outputToChunk.SetSocket(newChunkServerSocket);
            mPendingMessages.push(outputToChunk);
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
            MakeNewChunk(chunkFilename, System.currentTimeMillis() / 1000);
        }

        public void MakeNewChunk(String chunkFilename, long timeStamp) {
            try {
                if (chunkFilename.startsWith("/")) {
                    chunkFilename = chunkFilename.substring(1, chunkFilename.length());
                }
                if (mChunks.get(chunkFilename) == null) {
                    chunkFilename = chunkFilename.replaceAll("/", ".");
                    System.out.println("Making new chunk: " + chunkFilename);
                    mChunks.put(chunkFilename, new Chunk(chunkFilename, timeStamp));
                } else {
                    //System.out.println("Chunk already exists in mChunks wtf");
                    System.out.println("Chunk already exists");
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
                    String chunkData[] = line.split(" ");
                    MakeNewChunk(chunkData[0], Long.parseLong(chunkData[1]));
                }
                br.close();

                //tell server what chunks I have
                System.out.println("Telling server what chunks i have");
                Message chunksToServer = new Message();
                chunksToServer.WriteString("chunksihave");
                chunksToServer.WriteString(mID);
                chunksToServer.WriteInt(mChunks.values().size());
                for (Chunk c : mChunks.values()) {
                    String modifiedChunkFilename = c.mChunkFileName.replaceAll("\\.", "/");
                    chunksToServer.WriteString(modifiedChunkFilename);
                    chunksToServer.WriteLong(c.mLastModified);
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
                    pw.write(c.mChunkFileName + " " + c.mLastModified + "\r\n");
                }
                pw.close();
            } catch (IOException ie) {
                System.out.println("Unable to write to TFS structure file");
            }
        }

        public Message ParseChunkInput(Message m) {
            Message outputToChunk = new Message();
            String input = m.ReadString();
            System.out.println("Parsing chunk input " + input);
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
                case "cs-updatefile":
                    EvaluateCSUpdateFile(m, outputToChunk);
                    break;
                case "cs-updatefileresponse":
                    EvaluateCSUpdateFileResponse(m);
                    SaveFileStructure();
                    break;
                default:
                    System.out.println("chunk gave chunk server wrong command");
                    break;
            }
            System.out.println("Finished chunk input");
            return outputToChunk;
        }

        public void EvaluateCSUpdateFileResponse(Message m) {
            String fileName = m.ReadString();
            long lastModified = m.ReadLong();
            byte[] fileData = m.ReadData(m.ReadInt());
            if (!mChunks.containsKey(fileName)) {
                System.out.println("Received update to chunk I don't have");
                return;
            }
            mChunks.get(fileName).Overwrite(fileData);
            mChunks.get(fileName).mLastModified = lastModified;
        }

        public void EvaluateCSUpdateFile(Message m, Message output) {
            //primary receives this from the server that needs to update its files
            //filename
            String fileName = m.ReadString();
            System.out.println("someone needs to update file: " + fileName);
            if (!mChunks.containsKey(fileName)) {
                output.WriteDebugStatement("Server: " + mID + " does not have this chunk");
                return;
            }
            Chunk requestedChunk = mChunks.get(fileName);
            byte[] fileData = requestedChunk.ReadAllBytes();
            if (fileData != null) {
                output.WriteString("cs-updatefileresponse");
                output.WriteString(fileName);
                output.WriteLong(requestedChunk.mLastModified);
                output.WriteInt(fileData.length);
                System.out.println(fileData.length);
                output.AppendData(fileData);
            } else {
                output.WriteDebugStatement("Unable to read file: " + fileName);
            }
            //requestedChunk.ReadFrom(mClientNum, mClientNum, outData)
        }

        public void EvaluateCSAppendFile(Message m, Message output) {
            System.out.println("Got append request from another chunk server");
            String clientID = m.ReadString();
            String filename = m.ReadString();
            int dataSize = m.ReadInt();
            byte[] data = m.ReadData(dataSize);
            System.out.println("For file: " + filename);
            output.WriteString("cs-appendfileresponse");
            output.WriteString(clientID);
            output.WriteString(filename);
            output.WriteString(mID);
            output.WriteInt(AppendToFile(filename, data) ? 1 : 0);
            output.WriteLong(mChunks.get(filename).mLastModified);
        }

        public void EvaluateCSAppendFileResponse(Message m) {
            System.out.println("Received append file response");
            String clientID = m.ReadString();
            String filename = m.ReadString();
            String serverInfo = m.ReadString();
            int success = m.ReadInt();
            long timestamp = m.ReadLong();
            ChunkPendingAppend chunkService = mChunkServices.get(clientID + "-" + filename);
            chunkService.UpdateServerStatus(serverInfo, (success == 1), filename);
            chunkService.UpdateTimeStamp(timestamp);
            if (chunkService.AllServersReplied()) {
                System.out.println("Chunk server finished append");
                if (chunkService.WasSuccessful() == false) {
                    System.out.println("Not all servers were successful");
                    //chunkService.resendMessage();
                } else {
                    System.out.println("All servers were successful");
                    //send success to client
                    //send servermaster the timestamp for the chunk
                    Message toServerMaster = new Message();
                    toServerMaster.WriteString("appendtimestamp");
                    toServerMaster.WriteString(filename);
                    toServerMaster.WriteLong(timestamp);
                    toServerMaster.SetSocket(mServerSocket);
                    mPendingMessages.push(toServerMaster);
                    mChunkServices.remove(clientID + "-" + filename);
                }
            }
            //need to check if the server is not in the chunkservices (which should never ever happen)
        }

        public Message ParseClientInput(Message m) {
            Message outputToClient = new Message();
            String input = m.ReadString();
            System.out.println("Parsing client input " + input);
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
                case "logicalfilecount":
                    LogicalFileCount(m.ReadString(), outputToClient);
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
                byte[] dataWithoutSizeBytes = new byte[data.length - 4];
                System.arraycopy(data, 4, dataWithoutSizeBytes, 0, data.length - 4);
                output.WriteString("cs-readfileresponse");
                output.WriteString(fileName);
                output.WriteInt(dataWithoutSizeBytes.length);
                output.AppendData(dataWithoutSizeBytes);
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
            String clientID = input.ReadString();
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
            if (replicaInfo.length > 0) {
                ChunkPendingAppend mChunkService = mChunkServices.get(clientID + "-" + filename);
                if (mChunkService == null) {
                    //I should be the primary, but I don't have the service request yet
                    //make a new one and assume that the server will tell me i'm primary later
                    System.out.println("Making a a new chunkservicerequest.");
                    mChunkService = new ChunkPendingAppend(filename);
                    mChunkServices.put(clientID + "-" + filename, mChunkService);
                } else {
                    System.out.println("Using old chunkpendingappend.  shouldn't happen.  might happen with multiple clients");
                }
                for (String s : replicaInfo) {
                    System.out.println(s);
                    try {
                        synchronized (mMain.mChunkServers) {
                            MySocket newChunkServerSocket;
                            if (!mMain.mChunkServers.containsKey(s)) {
                                newChunkServerSocket = new MySocket(s);
                                InitConnectionWithChunkServer(newChunkServerSocket);
                                mChunkServers.put(s, newChunkServerSocket);
                                mChunkService.AddServer(newChunkServerSocket);
                            } else {
                                newChunkServerSocket = mMain.mChunkServers.get(s);
                                mChunkService.AddServer(newChunkServerSocket);
                            }
                        }
                    } catch (IOException ioe) {
                        System.out.println(ioe.getMessage());

                    }
                }
                AppendToFile(filename, inData);
                mChunkService.UpdateTimeStamp(mChunks.get(filename).mLastModified);
                mChunkService.GetMessage().WriteString("cs-appendreplica");
                mChunkService.GetMessage().WriteString(clientID);
                mChunkService.GetMessage().WriteString(filename);
                mChunkService.GetMessage().WriteInt(inData.length);
                mChunkService.GetMessage().AppendData(inData);
                try {
                    mChunkService.SendRequest();
                } catch (IOException ioe) {
                    System.out.println("Had problem writing data to socket");
                }
            } else {
                AppendToFile(filename, inData);
                Message toServerMaster = new Message();
                toServerMaster.WriteString("appendtimestamp");
                toServerMaster.WriteString(filename);
                toServerMaster.WriteLong(mChunks.get(filename).mLastModified);
                toServerMaster.SetSocket(mServerSocket);
                mPendingMessages.push(toServerMaster);
            }
        }

        public void DeleteFile(String filePath) {
            try {
                if (filePath.indexOf("/") == 0) {
                    filePath = filePath.substring(1);
                }
                String chunkPath = filePath.replace("/", ".");
                // remove virtual file from chunk server
                if (mChunks.get(chunkPath) != null) {
                    mChunks.remove(chunkPath);
                    // delete physical file
                    Path path = FileSystems.getDefault().getPath(chunkPath);
                    System.out.println("Deleting file " + path + " from chunk server");
                    Files.deleteIfExists(path);
                }
            } catch (IOException ie) {
                System.out.println("Had problem deleting file from chunk server");
            }
        }

        public void LogicalFileCount(String fileName, Message output) {
            try {
                fileName = fileName.replaceAll("/", ".");
                System.out.println("counting file " + fileName);
                Path filePath = Paths.get(fileName);
                File f = new File(fileName);
                int numFiles = 0;
                RandomAccessFile raf = new RandomAccessFile(f, "rw");

                if (f.exists()) {
                    //BufferedInputStream in = new BufferedInputStream(Files.newInputStream(filePath));
                    //byte[] intByte = new byte[4];
                    //in.mark(0);
                    int skippedBytes = 0;
                    while (raf.getFilePointer() < raf.length()) {
                        //in.reset(); //move back to the point before the byte
                        //in.read(intByte);
                        int subFilesize = raf.readInt();
                        raf.skipBytes(subFilesize);
                        skippedBytes += subFilesize;
                        numFiles++;

                    }
                    output.WriteString("cs-logicalfilecountresponse");
                    output.WriteInt(numFiles);
                    System.out.println("Skipped: " + skippedBytes + "bytes");
                } else {
                    output.WriteDebugStatement("File " + fileName + " does not exist");
                }
            } catch (IOException ioe) {
                System.out.println(ioe.getMessage());
            }
        }
    }
}
