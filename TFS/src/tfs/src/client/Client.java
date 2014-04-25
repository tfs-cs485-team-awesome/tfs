/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.client;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import static java.nio.file.StandardOpenOption.CREATE;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Stack;
import tfs.util.FileNode;
import tfs.util.Message;
import tfs.util.MySocket;
import tfs.util.HeartbeatSocket;
import tfs.util.ChunkQueryRequest;
import tfs.util.Callbackable;
/**
 *
 * @author laurencewong
 */
public class Client implements ClientInterface, Callbackable{

    Stack<String> mCurrentPath;
    String mID;
    String mServerIp;
    String sentence = "";
    int mServerPortNum;
    MySocket serverSocket;
    ArrayList<MySocket> mChunkServerSockets;
    ArrayDeque<ChunkQueryRequest> mPendingChunkQueries;
    ArrayDeque<Message> mPendingMessages;
    ArrayDeque<String> mSocketsToClose;
    HeartbeatSocket mHeartbeatSocket;

    //TEMP CODE
    String[] mTempFilesUnderNode;
    FileNode mTempFileNode;
    //END TEMP CODE

    /**
     *
     * @param mServerInfo a string that MUST be in the format ip:port
     */
    public Client(String mServerInfo) {
        String[] parts = mServerInfo.split(":");
        if (parts.length != 2) {
            System.out.println("Server information " + mServerInfo + " is in wrong format");
            return;
        }

        try {
        mServerIp = InetAddress.getByName(parts[0]).toString();
        mServerIp = mServerIp.substring(mServerIp.indexOf("/") + 1);
        } catch (UnknownHostException uhe) {
            System.out.println("mServerip = " + mServerIp);
            System.out.println("Unable to get serverIP; defaulting to localhost");
            mServerIp = "localhost";
        }
        mServerPortNum = Integer.valueOf(parts[1]);
        mCurrentPath = new Stack<>();
        mCurrentPath.add("");
        mChunkServerSockets = new ArrayList<>();
        mPendingChunkQueries = new ArrayDeque<>();
        mPendingMessages = new ArrayDeque<>();
        mSocketsToClose = new ArrayDeque<>();
    }

    public final String GetIP() throws IOException {
        String ip = InetAddress.getLocalHost().toString();
        ip = ip.substring(ip.indexOf("/") + 1);
        return ip;
    }

    public void InitConnectionWithMasterServer(MySocket inSocket) {
        try {
            System.out.println("Telling master server that I am a client");
            Message toServer = new Message();
            toServer.WriteString("Client");
            inSocket.WriteMessage(toServer);
            while (!inSocket.hasData()) {
            }
            Message fromServer = new Message(inSocket.ReadBytes());
            if (!fromServer.ReadString().equalsIgnoreCase("setid")) {
                System.out.println("Did not get setid from server.  Shutting down");
                System.exit(1);
            } else {
                mID = fromServer.ReadString();
                System.out.println("Setting id to " + mID);
                mHeartbeatSocket = new HeartbeatSocket(mID, serverSocket.GetID(), this);
                mHeartbeatSocket.start();
            }
        } catch (IOException ioExcept) {
            System.out.println("Problem initializing connection with server");
            System.out.println(ioExcept.getMessage());
        }
    }

    public void InitConnectionWithChunkServer(MySocket inSocket) {
        try {
            System.out.println("Telling chunk server that I am a client");
            Message toServer = new Message();
            toServer.WriteString("Client");
            toServer.WriteString(mID);
            toServer.WriteString(mHeartbeatSocket.GetIPAndPort());
            inSocket.WriteMessage(toServer);
        } catch (IOException ioExcept) {
            System.out.println("Problem initializing connection with server");
            System.out.println(ioExcept.getMessage());
        }
    }

    public ChunkQueryRequest GetRequestWithFilename(String fileName) {
        for (ChunkQueryRequest cqr : mPendingChunkQueries) {
            if (cqr.GetName().equalsIgnoreCase(fileName)) {
                return cqr;
            }
        }
        return null;
    }

    /**
     *
     */
    public boolean ReceiveMessage() throws IOException, UnknownHostException {
        if (serverSocket.hasData()) {
            Message fromServer = new Message(serverSocket.ReadBytes());
            while (!fromServer.isFinished()) {
                //message has data
                ParseServerInput(fromServer);
            }
            return true;
        }
        for (MySocket chunkSocket : mChunkServerSockets) {
            if (chunkSocket.hasData()) {
                Message fromChunk = new Message(chunkSocket.ReadBytes());
                while (!fromChunk.isFinished()) {
                    ParseChunkInput(fromChunk);
                }
            }
        }
        return false;
    }

    public boolean SendMessage() throws IOException {
        Message toServer = new Message();

        if (!sentence.isEmpty()) {
            String[] sentenceTokenized = sentence.split(" ");
            if (ParseUserInput(sentenceTokenized, toServer)) {
                serverSocket.WriteMessage(toServer);
                sentence = "";
                return true;
            }
        }
        while (!mPendingMessages.isEmpty()) {
            mPendingMessages.pop().Send();
        }
        sentence = "";
        return false;
    }

    public void ConnectToServer() throws IOException {
        serverSocket = new MySocket(mServerIp, mServerPortNum);
        InitConnectionWithMasterServer(serverSocket);
    }

    public MySocket GetSocketForID(String serverInfo) {
        for (MySocket ms : mChunkServerSockets) {
            if (ms.GetID().equalsIgnoreCase(serverInfo)) {
                return ms;
            }
        }
        if(serverSocket.GetID().equalsIgnoreCase(serverInfo) || serverInfo.equalsIgnoreCase("servermaster")) {
            return serverSocket;
        }
        return null;
    }
    
    public void RemoveSocket(MySocket socket) {
        if(!mChunkServerSockets.remove(socket)) {
            if(serverSocket.GetID().equalsIgnoreCase(socket.GetID())) {
                //remove serverSocket
            }
        }
    }

    public void RunLoop() {
        String modifiedSentence = "";
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Starting client on ip: " + mServerIp + " and port: " + mServerPortNum);
        while (true) {
            try {
                ConnectToServer();

                while (true) {

                    if (System.in.available() > 0) {
                        sentence = inFromUser.readLine();
                    }
                    if (false /*eventually add heartbeat message check in here*/) {
                        break;
                    }
                    try {
                    SendMessage();
                    } catch (IOException e) {
                        System.out.println(e.getMessage());
                    }
                    ReceiveMessage();
                    synchronized(mSocketsToClose) {
                        while (!mSocketsToClose.isEmpty()) {
                            String socketID = mSocketsToClose.pop();
                            MySocket socketToClose = GetSocketForID(socketID);
                            if(socketToClose == null) {
                                System.out.println("Cannot find socket: " + socketID);
                                continue;
                            }
                            System.out.println("Closing socket " + socketID);
                            if(socketToClose.GetID().equalsIgnoreCase(serverSocket.GetID())) {
                                break;
                            }
                            socketToClose.close();
                            RemoveSocket(socketToClose);
                        }
                    }
                }
                serverSocket.close();
            } catch (IOException e) {
                if (e.getMessage().contentEquals("Connection refused")) {
                    System.out.println("Server is not online.  Attempting to reconnect in 3s");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e1) {
                        System.out.println("Exception sleeping");
                        System.out.println(e1.getMessage());
                    }
                }
            }
        }
    }

    public boolean ParseUserInput(String[] inStrings, Message toServer) {
        String command = inStrings[0];
        switch (command.toLowerCase()) {
            case "createnewdirectory":
            case "mkdir":
                return ParseCreateNewDir(inStrings, toServer);
            case "createnewfile":
            case "touch":
                return ParseCreateNewFile(inStrings, toServer);
            case "deletefile":
            case "rm":
                return ParseDeleteFile(inStrings, toServer);
            case "listfiles":
            case "ls":
                return ParseListFiles(inStrings, toServer);
            case "seekfile":
            case "seek":
                return ParseSeekFile(inStrings, toServer);
            case "readfile":
            case "read":
                return ParseReadFile(inStrings, toServer);
            case "append":
            case "appendtofile":
                return ParseAppendFileToFile(inStrings, toServer);
            case "writetofile":
            case "write":
                return ParseWriteToFile(inStrings, toServer);
            case "getnode":
                return ParseGetNode(inStrings, toServer);
            case "getfilesunderpath":
                return ParseFilesUnderNode(inStrings, toServer);
            case "cd":
            case "dir":
                return ParseTestPath(inStrings, toServer);
            case "pwd":
                System.out.println(GetCurrentPath());
                return false;
            case "logicalfilecount":
                return ParseLogicalFileCount(inStrings, toServer);
            case "stream":
                return ParseStreamToFile(inStrings, toServer);
            default:
                System.out.println("Unknown command" + inStrings[0]);
                return false;
        }
    }

    public String GetCurrentPath() {
        String returnString = "";
        for (String s : mCurrentPath) {
            returnString += s + "/";
        }
        return returnString;
    }

    public boolean ParseStreamToFile(String[] inString, Message toServer) {
        //stream <text you're streaming> filename chunkNum

        if (inString.length < 3) {
            System.out.println("Too few arguments");
            return false;
        }

        String giantString = new String();
        for (String s : inString) {
            giantString += " " + s;
        }
        if (giantString.contains("\"") && !(giantString.indexOf("\"") == giantString.lastIndexOf("\"")) && !giantString.endsWith("\"")) {
            String textToStream = giantString.substring(giantString.indexOf("\"") + 1, giantString.lastIndexOf("\""));
            String[] newInString = new String[3];
            newInString[0] = inString[0]; //should be stream
            newInString[1] = textToStream;
            newInString[2] = inString[inString.length - 1];
            inString = newInString;
        }

        if (inString.length != 3) {
            System.out.println("Invalid number of parameters");
            return false;
        }

        ChunkQueryRequest newQuery = new ChunkQueryRequest(inString[2], ChunkQueryRequest.QueryType.APPEND);
        newQuery.PutData(inString[1].getBytes());
        mPendingChunkQueries.push(newQuery);

        //parse quotes if they exist
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[2]);
        return true;

    }

    public boolean ParseLogicalFileCount(String[] inString, Message toServer) {
        if (inString.length != 2) {
            System.out.println("Invalid number of parameters");
            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    public boolean ParseTestPath(String[] inString, Message toServer) {
        if (inString.length != 2) {
            return false;
        }
        if (inString[1].contentEquals("..")) {
            if (mCurrentPath.size() > 1) {
                mCurrentPath.pop();
                return true;
            } else {
                System.out.println("At top of filesystem");
                return false;
            }
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(GetCurrentPath() + inString[1]);
        mCurrentPath.push(inString[1]);
        return true;
    }

    public boolean ParseFilesUnderNode(String[] inString, Message toServer) {
        if (inString.length != 2) {

            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    public boolean ParseGetNode(String[] inString, Message toServer) {
        if (inString.length != 2) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    public boolean ParseWriteStringToFile(String[] inString, Message toServer) {
        //cmd filename len data
        if (inString.length != 4) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);

        toServer.WriteString(inString[1]);

        int sizeOfData = 0;
        if (!inString[2].matches("[0-9]+")) {
            System.out.println("Invalid argument type");
            return false;
        } else {
            toServer.WriteInt(Integer.valueOf(inString[2]));
            sizeOfData = Integer.valueOf(inString[2]);
        }

        //write input as bytes
        byte[] dataToWrite = new byte[1];
        for (int i = 0; i < sizeOfData; ++i) {
            dataToWrite[0] = (byte) inString[3].charAt(i);
            toServer.AppendData(dataToWrite);

        }
        return true;
    }

    public boolean ParseAppendFileToFile(String[] inString, Message toServer) {
        //cmd local remote 
        if (inString.length != 3) {
            System.out.println("Invalid number of arguments");
            return false;
        }

        toServer.WriteString(inString[0]);

        toServer.WriteString(inString[2]);

        //it's a file
        //handle this sometime
        try {
            Path filePath = Paths.get(inString[1]);
            byte[] data = Files.readAllBytes(filePath);
            toServer.WriteInt(data.length);
            toServer.AppendData(data);

            ChunkQueryRequest newQuery = new ChunkQueryRequest(inString[2], ChunkQueryRequest.QueryType.APPEND);
            newQuery.PutData(data);
            mPendingChunkQueries.push(newQuery);

        } catch (IOException ie) {
            System.out.println("Unable to read local file");
            return false;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return true;
    }

    public boolean ParseWriteToFile(String[] inString, Message toServer) {
        //cmd localfile remotefile
        if (inString.length != 4) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);

        toServer.WriteString(inString[2]);
        //toServer.WriteInt(Integer.valueOf(inString[3])); // number of replicas

        //it's a file
        //handle this sometime
        try {
            Path filePath = Paths.get(inString[1]);
            byte[] data = Files.readAllBytes(filePath);
            toServer.WriteInt(data.length);
            toServer.AppendData(data);

        } catch (IOException ie) {
            System.out.println("Unable to read local file");
            return false;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return true;
    }

    public boolean ParseReadFile(String[] inString, Message toServer) {
        //cmd remotefilename localfilename

        if (inString.length != 3) {
            System.out.println("Invalid number of arguments");
            return false;
        }

        File tempFile = new File(inString[2]);
        if (tempFile.exists()) {
            System.out.println("Local file " + inString[2] + " already exists.  Aborting");
            return false;
        }

        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        //toServer.WriteString(inString[2]);

        ChunkQueryRequest newChunkQueryRequest = new ChunkQueryRequest(inString[1], ChunkQueryRequest.QueryType.READ);
        newChunkQueryRequest.PutData(inString[2].getBytes());
        mPendingChunkQueries.push(newChunkQueryRequest);

        return true;
    }

    public boolean ParseSeekFile(String[] inString, Message toServer) {
        //cmd filename offset len

        if (inString.length != 4) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);

        toServer.WriteString(inString[1]);

        int sizeOfData = 0;
        if (!inString[2].matches("[0-9]+")) {
            System.out.println("Invalid argument type");
            return false;
        } else {
            toServer.WriteInt(Integer.valueOf(inString[2]));
            sizeOfData = Integer.valueOf(inString[2]);
        }

        if (!inString[3].matches("[0-9]+")) {
            System.out.println("Invalid argument type");
            return false;
        } else {
            toServer.WriteInt(Integer.valueOf(inString[3]));
        }
        return true;
    }

    public boolean ParseListFiles(String[] inString, Message toServer) {
        if (inString.length > 2) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);

        if (inString.length == 1) {
            toServer.WriteString(GetCurrentPath());
        } else {
            toServer.WriteString(inString[1]);
        }
        return true;
    }

    public boolean ParseDeleteFile(String[] inString, Message toServer) {
        if (inString.length != 2) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    public boolean ParseCreateNewFile(String[] inString, Message toServer) {
        if (inString.length != 2) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    public boolean ParseCreateNewDir(String[] inString, Message toServer) {
        if (inString.length != 2) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    public void ParseServerInput(Message m) throws IOException, UnknownHostException {
        String input = m.ReadString();
        switch (input.toLowerCase()) {
            case "print":
                //print out a debug statement
                System.out.println(m.ReadString());
                break;
            case "sm-appendresponse":
                SMAppendFileResponse(m);
            case "sm-writefileresponse":
                WriteFileResponse(m);
                break;
            case "sm-seekfileresponse":
                //name datalen data
                SeekFileResponse(m);
                break;
            case "sm-getnoderesponse": {
                //try {
                    mTempFileNode = new FileNode(false);
                    //mTempFileNode.ReadFromMessage(m);

//                } catch (IOException ioe) {
//                    System.out.println("Problem deserializing file node");
//                    System.out.println(ioe.getMessage());
//                }
                break;
            }
            case "sm-getfilesunderpathresponse": {
                mTempFilesUnderNode = GetFilesUnderNodeResponse(m);
                break;
            }
            case "sm-readfileresponse":
                SMReadFileResponse(m);
                break;
            case "sm-testpathresponse":
                TestPathResponse(m);
                break;
            case "sm-logicalfilecountresponse":
                LogicalFileCountResponse(m);
                break;
            default:
                System.out.println("Client received unknown command: " + input + " from server");
        }
    }

    public void SMAppendFileResponse(Message m) throws UnknownHostException, IOException {
        //filename, primary info, num replicas, replicainfo
        String filename = m.ReadString();
        String primaryChunkInfo = m.ReadString();
        System.out.println("Append primary info " + primaryChunkInfo);
        int numReplicas = m.ReadInt();
        String[] replicaInfo = new String[numReplicas];
        for (int i = 0; i < numReplicas; ++i) {
            replicaInfo[i] = m.ReadString();
        }

        MySocket newChunkServerSocket = GetSocketForID(primaryChunkInfo);
        if (newChunkServerSocket == null) {
            newChunkServerSocket = new MySocket(primaryChunkInfo);
            InitConnectionWithChunkServer(newChunkServerSocket);
            mChunkServerSockets.add(newChunkServerSocket);
        }

        Message toPrimaryChunkServer = new Message();
        toPrimaryChunkServer.WriteString("appendfile");
        toPrimaryChunkServer.WriteString(filename);
        toPrimaryChunkServer.WriteInt(numReplicas);
        for (int i = 0; i < numReplicas; ++i) {
            toPrimaryChunkServer.WriteString(replicaInfo[i]);
        }
        toPrimaryChunkServer.WriteInt(GetRequestWithFilename(filename).GetData().length);
        toPrimaryChunkServer.AppendData(GetRequestWithFilename(filename).GetData());
        toPrimaryChunkServer.SetSocket(newChunkServerSocket);
        mPendingMessages.push(toPrimaryChunkServer);

    }

    public void SMWriteFileResponse(Message m) throws UnknownHostException, IOException {
        String filename = m.ReadString();
        String primaryChunkInfo = m.ReadString();
        int numReplicas = m.ReadInt();
        String[] replicaInfo = new String[numReplicas];
        for (int i = 0; i < numReplicas; ++i) {
            replicaInfo[i] = m.ReadString();
        }

        MySocket newChunkServerSocket = GetSocketForID(primaryChunkInfo);
        if (newChunkServerSocket == null) {
            newChunkServerSocket = new MySocket(primaryChunkInfo);
            InitConnectionWithChunkServer(newChunkServerSocket);
        }

        mChunkServerSockets.add(newChunkServerSocket);

        Message toPrimaryChunkServer = new Message();
        toPrimaryChunkServer.WriteString("appendfile");
        toPrimaryChunkServer.WriteString(filename);
        toPrimaryChunkServer.WriteInt(numReplicas);
        for (int i = 0; i < numReplicas; ++i) {
            toPrimaryChunkServer.WriteString(replicaInfo[i]);
        }
        toPrimaryChunkServer.WriteInt(GetRequestWithFilename(filename).GetData().length);
        toPrimaryChunkServer.AppendData(GetRequestWithFilename(filename).GetData());
        toPrimaryChunkServer.SetSocket(newChunkServerSocket);
        mPendingMessages.push(toPrimaryChunkServer);
    }

    public void LogicalFileCountResponse(Message m) {
        //numfiles filesize filedata...
        int numFiles = m.ReadInt();
        System.out.println("Number of files in haystack: " + numFiles);
    }

    public void TestPathResponse(Message m) {
        int result = m.ReadInt();
        if (result != 1) {
            mCurrentPath.pop();
        }
    }

    public String[] GetFilesUnderNodeResponse(Message m) {
        int numStrings = m.ReadInt();
        String[] returnStrings = new String[numStrings];
        for (int i = 0; i < numStrings; ++i) {
            returnStrings[i] = m.ReadString();
        }
        return returnStrings;
    }

    public void SMReadFileResponse(Message m) throws IOException {
        System.out.println("Got smreadfileresponse");
        String filename = m.ReadString();
        String primaryChunkInfo = m.ReadString();
        System.out.println("Read primary info: " + primaryChunkInfo);
        int numReplicas = m.ReadInt();
        String[] replicaInfo = new String[numReplicas];
        for (int i = 0; i < numReplicas; ++i) {
            replicaInfo[i] = m.ReadString();
        }

        MySocket newChunkServerSocket = GetSocketForID(primaryChunkInfo);
        if (newChunkServerSocket == null) {
            System.out.println("Making new chunk server socket");
            newChunkServerSocket = new MySocket(primaryChunkInfo);
            InitConnectionWithChunkServer(newChunkServerSocket);
            mChunkServerSockets.add(newChunkServerSocket);
        }
        System.out.println("Writing message to chunk server");  
        Message toPrimaryChunkServer = new Message();
        toPrimaryChunkServer.WriteString("readfile");
        toPrimaryChunkServer.WriteString(GetRequestWithFilename(filename).GetName());
        toPrimaryChunkServer.WriteInt(numReplicas);
        for (int i = 0; i < numReplicas; ++i) {
            toPrimaryChunkServer.WriteString(replicaInfo[i]);
        }
        toPrimaryChunkServer.SetSocket(newChunkServerSocket);
        mPendingMessages.push(toPrimaryChunkServer);

        /*
         String filename = m.ReadString();
         int bytesToRead = m.ReadInt();
         byte[] bytesRead = m.ReadData(bytesToRead);
         WriteLocalFile(filename, bytesRead);*/
    }

    public void WriteFileResponse(Message m) {
        // if master returns chunk to be created, contact chunk server to create chunks

        // after getting chunks, append to end of chunk
    }

    public void SeekFileResponse(Message m) throws IOException {
        String filename = m.ReadString();
        int bytesToRead = m.ReadInt();
        byte[] bytesRead = m.ReadData(bytesToRead);
        WriteLocalFile(filename, bytesRead);
    }

    public void ParseChunkInput(Message m) throws IOException, UnknownHostException {
        String input = m.ReadString();
        System.out.println("Read " + input + " from chunk server");
        switch (input) {
            case "cs-readfileresponse":
                CSReadFileResponse(m);
                break;
        }
    }

    public void CSReadFileResponse(Message m) throws IOException {
        System.out.println("Got csreadfileresponse");
        String filename = m.ReadString();
        ChunkQueryRequest chunkQuery = GetRequestWithFilename(filename);
        if (chunkQuery == null) {
            System.out.println("Query from chunk server did not match any on this client");
            return;
        }
        String localFilename = new String(chunkQuery.GetData());
        int dataSize = m.ReadInt();
        byte[] data = m.ReadData(dataSize);
        WriteLocalFile(localFilename, data);
    }

    @Override
    public void CreateFile(String fileName) throws IOException {
        sentence = "touch " + fileName;
        if (SendMessage()) {
            while (!ReceiveMessage());
        }

    }

    @Override
    public void CreateDir(String dirName) throws IOException {
        sentence = "mkdir " + dirName;
        if (SendMessage()) {
            while (!ReceiveMessage());
        }
    }

    @Override
    public void DeleteFile(String fileName) throws IOException {
        sentence = "rm " + fileName;
        if (SendMessage()) {
            while (!ReceiveMessage());
        }
    }

    @Override
    public void ListFile(String path) throws IOException {
        sentence = "ls " + path;
        System.out.println(sentence);
        if (SendMessage()) {
            while (!ReceiveMessage());
        }
    }

    @Override
    public String[] GetListFile(String path) throws IOException {
        sentence = "GetFilesUnderPath " + path;
        SendMessage();
        while (!ReceiveMessage());
        return mTempFilesUnderNode;
    }

    @Override
    public void ReadFile(String remotefilename, String localfilename) throws IOException {
        sentence = "read " + remotefilename + " " + localfilename;
        if (SendMessage()) {
            while (!ReceiveMessage());
        }
    }

    @Override
    public void WriteFile(String localfilename, String remotefilename) throws IOException {
        sentence = "write " + localfilename + " " + remotefilename;
        if (SendMessage()) {
            while (!ReceiveMessage());
        }
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void AppendFile(String localfilename, String remotefilename) throws IOException {
        sentence = "append " + localfilename + " " + remotefilename;
        if (SendMessage()) {
            while (!ReceiveMessage());
        }
    }

    @Override
    public FileNode GetAtFilePath(String path) throws IOException {
        sentence = "GetNode " + path;
        if (SendMessage()) {
            while (!ReceiveMessage());
        }
        return mTempFileNode;
    }

    @Override
    public void WriteLocalFile(String fileName, byte[] data) throws IOException {
        Path filePath = Paths.get(fileName);
        OutputStream out = new BufferedOutputStream(Files.newOutputStream(filePath, CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        out.write(data);
        out.close();
        System.out.println("Finished writing file");
    }

    @Override
    public void CountFiles(String remotename) throws IOException {
        sentence = "LogicalFileCount " + remotename;
        if (SendMessage()) {
            while (!ReceiveMessage());
        }

    }

    @Override
    public void Callback(String inParameter) {
        synchronized(mSocketsToClose) {
            mSocketsToClose.push(inParameter);
        }
    }

}
