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
 * Interacts with the master server through a socket to send requests.
 *
 * @author laurencewong
 */
public class Client implements ClientInterface, Callbackable {

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
    ArrayDeque<String> mTestInput;
    HeartbeatSocket mHeartbeatSocket;

    //TEMP CODE
    String[] mTempFilesUnderNode;
    boolean mTempFilesUnderNodeReady = false;
    FileNode mTempFileNode;
    boolean mTempFileNodeReady = false;
    //END TEMP CODE

    /**
     * Constructor to create a new client and connects the server's IP number
     * and port number.
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
        mTestInput = new ArrayDeque<>();
    }

    /**
     * Retrieves IP address of the local host.
     *
     * @return IP address as a string
     * @throws IOException
     */
    public final String GetIP() throws IOException {
        String ip = InetAddress.getLocalHost().toString();
        ip = ip.substring(ip.indexOf("/") + 1);
        return ip;
    }

    /**
     * Initializes a connection with the master server.
     *
     * @param inSocket socket used to connect with the master server
     */
    public void InitConnectionWithMasterServer(MySocket inSocket) {
        try {
            System.out.println("Telling master server that I am a client");
            Message toServer = new Message();
            toServer.WriteString("Client");
            toServer.WriteString(GetIP());
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

    /**
     * Initializes a connection with a chunk server.
     *
     * @param inSocket socket used to connect to a chunk server
     */
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

    /**
     * Retrieves a chunk server request with a specified file name.
     *
     * @param fileName name of the file to retrieve a request from
     * @return returns the retrieved chunk query request
     */
    public ChunkQueryRequest GetRequestWithFilename(String fileName) {
        for (ChunkQueryRequest cqr : mPendingChunkQueries) {
            if (cqr.GetName().equalsIgnoreCase(fileName)) {
                return cqr;
            }
        }
        return null;
    }

    /**
     * Checks whether there is a new message to retrieve and iterates through
     * each new message.
     *
     * @return true if there is new data, false if there is no new data
     * @throws IOException
     * @throws UnknownHostException
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

    /**
     * Appends a message to a dequeue to be sent to the master server.
     *
     * @return true if the message is successfully sent
     * @throws IOException
     */
    public boolean SendMessage() throws IOException {
        boolean sentMessage = false;
        if (!sentence.isEmpty()) {
            Message toServer = new Message();

            String[] sentenceTokenized = sentence.split(" ");
            if (ParseUserInput(sentenceTokenized, toServer)) {
                toServer.SetSocket(serverSocket);
                mPendingMessages.push(toServer);
                sentence = "";
            }
        }
        while (!mTestInput.isEmpty()) {
            Message toServer = new Message();
            String[] sentenceTokenized = mTestInput.pop().split(" ");
            if (ParseUserInput(sentenceTokenized, toServer)) {
                toServer.SetSocket(serverSocket);
                mPendingMessages.push(toServer);
            }
        }
        while (!mPendingMessages.isEmpty()) {
            mPendingMessages.pop().Send();
            sentMessage = true;
        }
        sentence = "";
        return sentMessage;
    }

    /**
     * Establishes a connection to the master server.
     *
     * @throws IOException
     */
    public void ConnectToServer() throws IOException {
        serverSocket = new MySocket(mServerIp, mServerPortNum);
        InitConnectionWithMasterServer(serverSocket);
    }

    /**
     * Retrieves the socket for a specified server.
     *
     * @param serverInfo information on which server to get the socket for
     * @return
     */
    public MySocket GetSocketForID(String serverInfo) {
        for (MySocket ms : mChunkServerSockets) {
            if (ms.GetID().equalsIgnoreCase(serverInfo)) {
                return ms;
            }
        }
        if (serverSocket.GetID().equalsIgnoreCase(serverInfo) || serverInfo.equalsIgnoreCase("servermaster")) {
            return serverSocket;
        }
        return null;
    }

    /**
     * Removes the client socket from the chunk server sockets.
     *
     * @param socket the specified socket to remove
     */
    public void RemoveSocket(MySocket socket) {
        if (!mChunkServerSockets.remove(socket)) {
            if (serverSocket.GetID().equalsIgnoreCase(socket.GetID())) {
                //remove serverSocket
            }
        }
    }

    /**
     * Constantly running loop that receives incoming messages and closes
     * pending sockets.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void RunLoop() throws IOException, InterruptedException {
        try {
            SendMessage();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        ReceiveMessage();
        synchronized (mSocketsToClose) {
            while (!mSocketsToClose.isEmpty()) {
                String socketID = mSocketsToClose.pop();
                MySocket socketToClose = GetSocketForID(socketID);
                if (socketToClose == null) {
                    System.out.println("Cannot find socket: " + socketID);
                    continue;
                }
                System.out.println("Closing socket " + socketID);
                if (socketToClose.GetID().equalsIgnoreCase(serverSocket.GetID())) {
                    break;
                }
                socketToClose.close();
                RemoveSocket(socketToClose);
            }
        }
        Thread.sleep(50);
    }

    /**
     * Begins a client and opens a connection to a master server.
     */
    public void Start() {
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Starting client on ip: " + mServerIp + " and port: " + mServerPortNum);
        while (true) {
            try {
                try {
                    ConnectToServer();
                    while (true) {
                        if (System.in.available() > 0) {
                            sentence = inFromUser.readLine();
                        }
                        RunLoop();
                    }
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
                } catch (InterruptedException ie) {
                    System.out.println(ie.getMessage());
                } finally {
                    serverSocket.close();
                }
            } catch (IOException ioe) {
                System.out.println(ioe.getMessage());
            }
        }
    }

    /**
     * Interprets user input and executes the corresponding command.
     *
     * @param inStrings user input string
     * @param toServer message to be sent to the server
     * @return true if command is executed successfully, false on failure
     */
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

    /**
     * Retrieves current directory path.
     *
     * @return string of the current path
     */
    public String GetCurrentPath() {
        String returnString = "";
        for (String s : mCurrentPath) {
            returnString += s + "/";
        }
        return returnString;
    }

    /**
     * Parses a string and appends it to a file.
     *
     * @param inString user input string
     * @param toServer message to send to server
     * @return returns true on success, false on failure
     */
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

    /**
     * Checks for valid parameters for running a logical file count
     *
     * @param inString user input string
     * @param toServer message to send to the server
     * @return true on success, false on failure
     */
    public boolean ParseLogicalFileCount(String[] inString, Message toServer) {
        if (inString.length != 2) {
            System.out.println("Invalid number of parameters");
            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    /**
     * Parses a path to see if it is valid
     *
     * @param inString user input string
     * @param toServer message to send to server
     * @return true on success
     */
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

    /**
     * Takes an user input and validates it for listing files.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
    public boolean ParseFilesUnderNode(String[] inString, Message toServer) {
        if (inString.length != 2) {

            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    /**
     * Validates user input for retrieving a node.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
    public boolean ParseGetNode(String[] inString, Message toServer) {
        if (inString.length != 2) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    /**
     * Verifies user input for writing a string to a file.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
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

    /**
     * Validates user input for appending a file to a file.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
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

    /**
     * Validates user input for writing to a file.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
    public boolean ParseWriteToFile(String[] inString, Message toServer) {
        //cmd localfile remotefile num_replicas
        if (inString.length != 4) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);

        toServer.WriteString(inString[2]);
        toServer.WriteInt(Integer.valueOf(inString[3])); // number of replicas

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

    /**
     * Validates user input for reading a file.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
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

    /**
     * Validates user input for seeking in a file.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
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

    /**
     * Validates user input for listing files in a directory.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
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

    /**
     * Validates user input for deleting a file.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
    public boolean ParseDeleteFile(String[] inString, Message toServer) {
        if (inString.length != 2) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    /**
     * Validates user input for creating a new file.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
    public boolean ParseCreateNewFile(String[] inString, Message toServer) {
        if (inString.length != 2) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    /**
     * Validates user input for creating a new directory.
     *
     * @param inString user input
     * @param toServer message to send to server
     * @return true on success
     */
    public boolean ParseCreateNewDir(String[] inString, Message toServer) {
        if (inString.length != 2) {
            System.out.println("Invalid number of arguments");
            return false;
        }
        toServer.WriteString(inString[0]);
        toServer.WriteString(inString[1]);
        return true;
    }

    /**
     * Parses a message from the server and calls corresponding action.
     *
     * @param m message to parse
     * @throws IOException
     * @throws UnknownHostException
     */
    public void ParseServerInput(Message m) throws IOException, UnknownHostException {
        String input = m.ReadString();
        switch (input.toLowerCase()) {
            case "print":
                //print out a debug statement
                System.out.println(m.ReadString());
                break;
            case "sm-appendresponse": {
                SMAppendFileResponse(m);
                break;
            }
            case "sm-writeresponse":
                SMAppendFileResponse(m);
                break;
            case "sm-seekfileresponse":
                //name datalen data
                SeekFileResponse(m);
                break;
            case "sm-getnoderesponse": {
                //try {
                mTempFileNode = new FileNode(false);
                mTempFileNodeReady = true;
                break;
            }
            case "sm-getfilesunderpathresponse": {
                mTempFilesUnderNode = GetFilesUnderNodeResponse(m);
                mTempFilesUnderNodeReady = true;
                break;
            }
            case "sm-readfileresponse":
                SMReadFileResponse(m);
                break;
            case "sm-logicalfilecountresponse":
                SMLogicalFileCountResponse(m);
                break;
            default:
                System.out.println("Client received unknown command: " + input + " from server");
        }
    }

    /**
     * Response from master server with results of a file append.
     *
     * @param m message from the server
     * @throws UnknownHostException
     * @throws IOException
     */
    public void SMAppendFileResponse(Message m) throws UnknownHostException, IOException {
        //filename, primary info, num replicas, replicainfo
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
            mChunkServerSockets.add(newChunkServerSocket);
        }

        Message toPrimaryChunkServer = new Message();
        toPrimaryChunkServer.WriteString("appendfile");
        toPrimaryChunkServer.WriteString(mID);
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

    /**
     * Response from the master server for a write file request.
     *
     * @param m message from the server
     * @throws UnknownHostException
     * @throws IOException
     */
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

    /**
     * Response from the master server for getting a logical file count.
     *
     * @param m message from the server
     * @throws UnknownHostException
     * @throws IOException
     */
    public void SMLogicalFileCountResponse(Message m) throws IOException {
        String fileName = m.ReadString();
        String primaryChunkInfo = m.ReadString();
        MySocket newChunkServerSocket = GetSocketForID(primaryChunkInfo);
        if (newChunkServerSocket == null) {
            System.out.println("Making new chunk server socket");
            newChunkServerSocket = new MySocket(primaryChunkInfo);
            InitConnectionWithChunkServer(newChunkServerSocket);
            mChunkServerSockets.add(newChunkServerSocket);
        }
        System.out.println("Writing message to chunk server");
        Message toPrimaryChunkServer = new Message();
        toPrimaryChunkServer.WriteString("logicalfilecount");
        toPrimaryChunkServer.WriteString(fileName);
        toPrimaryChunkServer.SetSocket(newChunkServerSocket);
        mPendingMessages.push(toPrimaryChunkServer);
    }

    /**
     * Response from server for request to get files under a node.
     *
     * @param m message from server
     * @return list of strings representing each file under a node
     */
    public String[] GetFilesUnderNodeResponse(Message m) {
        int numStrings = m.ReadInt();
        String[] returnStrings = new String[numStrings];
        for (int i = 0; i < numStrings; ++i) {
            returnStrings[i] = m.ReadString();
        }
        return returnStrings;
    }

    /**
     * Response from master server for a read file request.
     *
     * @param m message from the server
     * @throws IOException
     */
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
    }

    /**
     * Response from server for a seek file request.
     *
     * @param m message from server
     * @throws IOException
     */
    public void SeekFileResponse(Message m) throws IOException {
        String filename = m.ReadString();
        int bytesToRead = m.ReadInt();
        byte[] bytesRead = m.ReadData(bytesToRead);
        WriteLocalFile(filename, bytesRead);
    }

    /**
     * Parses a message from a chunk server and executes the corresponding
     * command.
     *
     * @param m message from a chunk server
     * @throws IOException
     * @throws UnknownHostException
     */
    public void ParseChunkInput(Message m) throws IOException, UnknownHostException {
        String input = m.ReadString();
        System.out.println("Read " + input + " from chunk server");
        switch (input) {
            case "cs-readfileresponse":
                CSReadFileResponse(m);
                break;
            case "cs-logicalfilecountresponse":
                CSLogicalFileCountResponse(m);
                break;
        }
    }

    /**
     * Response from chunk server for a read file.
     *
     * @param m message from the chunk server
     * @throws IOException
     */
    public void CSReadFileResponse(Message m) throws IOException {
        System.out.println("Got csreadfileresponse");
        String filename = m.ReadString();
        filename = filename.replaceAll("\\.", "/");
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

    /**
     * Response from chunk server for a logical file count.
     *
     * @param m message from the chunk server
     * @throws IOException
     */
    public void CSLogicalFileCountResponse(Message m) {
        System.out.println("Number of files in haystack: " + m.ReadInt());
    }

    @Override
    public void CreateFile(String fileName) throws IOException {
        mTestInput.push("touch " + fileName);
    }

    @Override
    public void CreateDir(String dirName) throws IOException {
        mTestInput.push("mkdir " + dirName);
    }

    @Override
    public void DeleteFile(String fileName) throws IOException {
        mTestInput.push("rm " + fileName);
    }

    @Override
    public void ListFile(String path) throws IOException {
        mTestInput.push("ls " + path);
    }

    @Override
    public void GetListFile(String path) throws IOException {
        mTestInput.push("GetFilesUnderPath " + path);
    }

    @Override
    public String[] GetListFile() throws IOException, InterruptedException {
        while (!mTempFilesUnderNodeReady) {
            RunLoop();
        }
        mTempFilesUnderNodeReady = false;
        return mTempFilesUnderNode;
    }

    @Override
    public void ReadFile(String remotefilename, String localfilename) throws IOException {
        mTestInput.push("read " + remotefilename + " " + localfilename);
    }

    @Override
    public void WriteFile(String localfilename, String remotefilename, int numReplicas) throws IOException {
        mTestInput.push("write " + localfilename + " " + remotefilename + " " + numReplicas);
    }

    @Override
    public void AppendFile(String localfilename, String remotefilename) throws IOException {
        mTestInput.push("append " + localfilename + " " + remotefilename);
    }

    @Override
    public void GetAtFilePath(String path) throws IOException {
        mTestInput.push("GetNode " + path);
    }

    @Override
    public FileNode GetAtFilePath() throws IOException, InterruptedException {
        while (!mTempFileNodeReady) {
            RunLoop();
        }
        mTempFileNodeReady = false;
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
        mTestInput.push("LogicalFileCount " + remotename);
    }

    @Override
    public void Callback(String inParameter) {
        synchronized (mSocketsToClose) {
            mSocketsToClose.push(inParameter);
        }
    }
}
