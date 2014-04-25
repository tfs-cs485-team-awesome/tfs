/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.servermaster;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Random;
import tfs.util.FileNode;
import tfs.util.Message;
import tfs.util.MySocket;
import tfs.util.HeartbeatSocket;
import tfs.util.Callbackable;

/**
 *
 * @author laurencewong
 */
public class ServerMaster implements Callbackable {

    FileNode mFileRoot;
    ServerSocket mListenSocket;
    HeartbeatSocket mHeartbeatSocket;
    final ArrayList<MySocket> mClients = new ArrayList<>();
    final ArrayList<MySocket> mChunkServers = new ArrayList<>();
    ArrayDeque<Message> mPendingMessages; //messages to send out
    ArrayDeque<String> mSocketsToClose;

    final int NUM_REPLICAS = 3;

    public final String GetIP() throws IOException {
        String ip = InetAddress.getLocalHost().toString();
        ip = ip.substring(ip.indexOf("/") + 1);
        return ip;
    }

    public final void Init() {
        mFileRoot = new FileNode(false);
        mFileRoot.mName = "/";
        mPendingMessages = new ArrayDeque<>();
        mSocketsToClose = new ArrayDeque<>();
    }

    public ServerMaster(int inSocketNum) {
        Init();
        try {
            System.out.println("Starting server on port " + inSocketNum);
            mListenSocket = new ServerSocket(inSocketNum);
            mHeartbeatSocket = new HeartbeatSocket(inSocketNum, this);
            mHeartbeatSocket.start();
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    public ServerMaster() {
        Init();
        try {
            mListenSocket = new ServerSocket(6789);
            mHeartbeatSocket = new HeartbeatSocket(6789, this);
            mHeartbeatSocket.start();
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    public void RunLoop() {
        System.out.println("Starting server");
        try {
            ServerMasterClientThread newClientThread = new ServerMasterClientThread(this);
            newClientThread.start();
            while (true) {
                MySocket newConnection = new MySocket(mListenSocket.accept());
                System.out.println("Accepting new connection");
                //BufferedReader newConnectionReader = new BufferedReader(new InputStreamReader(newConnection.getInputStream()));
                //String connectionType = newConnectionReader.readLine();
                Message m = new Message(newConnection.ReadBytes());
                String connectionType = m.ReadString();
                switch (connectionType) {
                    case "Client": {
                        synchronized (mClients) {
                            mClients.add(newConnection);
                        }

                        Message sendID = new Message();
                        sendID.WriteString("setid");
                        newConnection.SetID(newConnection.GetSocket().getInetAddress() + ":" + newConnection.GetSocket().getPort());
                        sendID.WriteString(newConnection.GetID());
                        newConnection.WriteMessage(sendID);
                        System.out.println("Adding new client");
                        break;
                    }
                    case "ChunkServer": {
                        synchronized (mChunkServers) {

                            System.out.println("Accepting chunk server" + newConnection.GetSocket().getLocalAddress() + ((InetSocketAddress) newConnection.GetSocket().getLocalSocketAddress()).getPort());
                            Message sendID = new Message();
                            sendID.WriteString("setid");
                            int ChunkServerListenPort = m.ReadInt();
                            newConnection.SetID(newConnection.GetID().split(":")[0] + ":" + ChunkServerListenPort);
                            sendID.WriteString(newConnection.GetID());
                            System.out.println("Adding new chunkserver with ID : " + newConnection.GetID());
                            newConnection.WriteMessage(sendID);
                            mChunkServers.add(newConnection);
                            break;
                        }
                    }
                    default:
                        System.out.println("Server was told new connection of type: " + connectionType);
                        break;
                }

            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println(e.getLocalizedMessage());
        }

        System.out.println(
                "Exiting server");
    }

    @Override
    public void Callback(String inParameter) {
        synchronized (mSocketsToClose) {
            mSocketsToClose.push(inParameter);

        }
    }

    public class ServerMasterClientThread extends Thread {

        ServerMaster mMaster;
        int mClientNum;

        public ServerMasterClientThread(ServerMaster inMaster) {
            mMaster = inMaster;
            System.out.println("Creating client serving thread");
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
                        for (MySocket chunkSocket : mChunkServers) {
                            if(chunkSocket.hasData()) {
                             System.out.println("Reading bytes from " + chunkSocket.GetSocket().getLocalAddress() + ((InetSocketAddress) chunkSocket.GetSocket().getLocalSocketAddress()).getPort());
                             Message messageReceived = new Message(chunkSocket.ReadBytes());
                             Message messageSending = ParseChunkInput(messageReceived);
                             messageSending.SetSocket(chunkSocket);
                             mPendingMessages.push(messageSending);
                            }
                        }
                    }

                    while (!mPendingMessages.isEmpty()) {
                        mPendingMessages.pop().Send();
                    }
                    synchronized (mSocketsToClose) {
                        while (!mSocketsToClose.isEmpty()) {
                            String socketID = mSocketsToClose.pop();
                            MySocket socketToClose = GetSocket(socketID);
                            if (socketToClose == null) {
                                System.out.println("Cannot find socket: " + socketID);
                                continue;
                            }
                            System.out.println("Closing socket " + socketID);
                            RemoveSocket(socketToClose);
                            //need to remove socket
                        }
                    }

                    this.sleep(100);
                } catch (Exception e) {

                    System.out.println(e.getMessage());
                    e.printStackTrace();
                }

            }
        }

        public MySocket GetSocket(String inSocketID) {
            //most likely chunk socket, so search those first
            for (MySocket s : mMaster.mChunkServers) {
                if (s.GetID().compareTo(inSocketID) == 0) {
                    return s;
                }
            }
            for (MySocket s : mMaster.mClients) {
                if (s.GetID().compareTo(inSocketID) == 0) {
                    return s;
                }
            }
            return null;
        }

        public void RemoveSocket(MySocket inSocket) {
            if (mChunkServers.remove(inSocket)) {
                RemoveLocationFromFiles(inSocket.GetID());
                return;
            }
            if (mClients.remove(inSocket)) {
                return;
            }
            System.out.println("Socket not found " + inSocket.GetID());
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
                    Message m = new Message();
                    String[] pair = line.split(" ");
                    if (pair[0].equals("DIRECTORY")) {
                        CreateNewSetupDir(pair[1]);
                    } else {
                        CreateNewSetupFile(pair[1]);
                    }
                }
                br.close();
            } catch (IOException ie) {
                System.out.println("Unable to read TFS structure file");
            }
        }

        public void SaveFileStructure(Boolean isDirectory, String name) {
            System.out.println("Saving file structure");
            FileNode file = GetAtPath(name);
            if (file != null) {
                return;
            }
            try {
                PrintWriter pw = new PrintWriter(new FileWriter("SYSTEM_LOG.txt", true));
                String newline;
                if (isDirectory) {
                    newline = "DIRECTORY " + name + "\n";
                } else {
                    newline = "FILE " + name + "\n";
                }
                pw.write(newline);
                pw.close();
            } catch (IOException ie) {
                System.out.println("Unable to write to TFS structure file");
            }
        }

        public void DeleteFromFileStructure(Boolean isDirectory, String name) {
            String lineToDelete;
            if (isDirectory) {
                lineToDelete = "DIRECTORY " + name;
            } else {
                lineToDelete = "FILE " + name;
            }
            File file = new File("SYSTEM_LOG.txt");
            if (!file.exists() || !file.isFile()) {
                System.out.println("Unable to delete from TFS file structure");
                return;
            }
            try {
                File tempFile = new File(file.getAbsolutePath() + ".tmp");
                BufferedReader br = new BufferedReader(new FileReader(file));
                PrintWriter pw = new PrintWriter(new FileWriter(tempFile));

                String line = null;
                while ((line = br.readLine()) != null) {
                    if (!line.trim().equals(lineToDelete)) {
                        pw.println(line);
                        pw.flush();
                    }
                }
                pw.close();
                br.close();
                if (!file.delete()) {
                    System.out.println("Could not delete file");
                    return;
                }
                if (!tempFile.renameTo(file)) {
                    System.out.println("Could not rename file");
                }
            } catch (IOException ie) {
                System.out.println("Failed to delete file from the TFS file structure");
            }
        }

        public void CreateNewSetupDir(String name) {
            // retrieve index of the last "/"
            int lastIndex = name.lastIndexOf("/");
            if (lastIndex < 0) {
                return;
            }
            // default parent node to the root node
            FileNode parentNode = mMaster.mFileRoot;
            // set parent node to the parent directory
            if (lastIndex > 1) {
                String parent = name.substring(0, lastIndex);
                parentNode = GetAtPath(parent);
                if (parentNode == null) {
                    return;
                }
            }
            // check for locks in the parent node
            if (parentNode.RequestWriteLock()) {
                try {
                    FileNode newDir = new FileNode(false);
                    newDir.mIsDirectory = true;
                    newDir.mName = name.substring(lastIndex + 1, name.length());
                    parentNode.mChildren.add(newDir);
                } finally {
                    parentNode.ReleaseWriteLock();
                }
            }            
        }

        public void CreateNewSetupFile(String name) {
            // retrieve index of the last "/"
            int lastIndex = name.lastIndexOf("/");
            if (lastIndex < 0) {
                return;
            }
            // default parent node to the root node
            FileNode parentNode = mMaster.mFileRoot;
            // set parent node to the parent directory
            if (lastIndex > 1) {
                String parent = name.substring(0, lastIndex);
                parentNode = GetAtPath(parent);
                if (parentNode == null) {
                    return;
                }
            }
            // create new file
            FileNode newFile = new FileNode(true);
            newFile.mIsDirectory = false;
            newFile.mName = name.substring(lastIndex + 1, name.length());
            parentNode.mChildren.add(newFile);
            return;
        }

        public Message ParseChunkInput(Message m) {
            Message outputToClient = new Message();

            String command = m.ReadString();
            System.out.println("Server receivd: " + command);
            switch (command.toLowerCase()) {
                case "chunksihave":
                    UpdateChunkLocations(m, outputToClient);
                    break;
            }
            return outputToClient;
        }

        public void UpdateChunkLocations(Message m, Message output) {
            String inChunkServerLocation = m.ReadString();
            int numChunks = m.ReadInt();
            for (int i = 0; i < numChunks; ++i) {
                String readPath = m.ReadString();
                FileNode fn = GetAtPath(readPath);
                if (fn == null) {
                    output.WriteDebugStatement("File " + readPath + " does not exist on the server");
                } else if (fn.mIsDirectory) {
                    output.WriteDebugStatement("File " + readPath + " is a directory on the server");
                } else {
                    fn.GetChunkDataAtIndex(0).SetChunkLocation(inChunkServerLocation);
                }
            }

        }

        /**
         * Parses the client's input into a command and a parameter
         *
         * @param input should be in the format "CommandName Parameters"
         */
        public Message ParseClientInput(Message m) {
            Message outputToClient = new Message();

            String command = m.ReadString();
            System.out.println("Server Received: " + command);
            outputToClient.WriteDebugStatement("Server Received " + command);
            switch (command.toLowerCase()) {
                case "createnewdirectory":
                case "mkdir":
                    CreateNewDir(m.ReadString(), outputToClient);
                    break;
                case "createnewfile":
                case "touch":
                    CreateNewFile(m.ReadString(), outputToClient);
                    break;
                case "deletefile":
                case "rm":
                    DeleteDirectory(m.ReadString(), outputToClient);
                    break;
                case "listfiles":
                case "ls":
                    ListFiles(m.ReadString(), outputToClient);
                    break;
                case "seekfile":
                case "seek":
                    SeekFile(m.ReadString(), m.ReadInt(), m.ReadInt(), outputToClient);
                    break;
                case "readfile":
                case "read":
                    //ReadFile(m.ReadString(), m.ReadString(), outputToClient);
                    ReadFile(m.ReadString(), outputToClient);
                    break;
                case "writefile":
                case "write": {
                    String name = m.ReadString();
                    int lengthToRead = m.ReadInt();
                    byte[] data = m.ReadData(lengthToRead);
                    WriteFile(name, data, outputToClient);
                    break;
                }
                case "stream":
                    AppendFile(m.ReadString(), outputToClient);
                    ///AppendFile(m.ReadString(), m.ReadString().getBytes(), outputToClient);
                    break;
                case "append":
                case "appendtofile":
                    AppendFile(m.ReadString(), outputToClient);
                    /*
                     String name = m.ReadString();
                     int lengthToRead = m.ReadInt();
                     byte[] data = m.ReadData(lengthToRead);
                     AppendFile(name, data, outputToClient);
                     */
                    break;
                case "getnode": {
                    String path = m.ReadString();
                    System.out.println("Getting node " + path);
                    outputToClient.WriteDebugStatement("Getting node " + path);
                    FileNode toClient = GetAtPath(path);
                    //try {
                        outputToClient.WriteString("GetNodeResponse");
                        //toClient.WriteToMessage(outputToClient);
//                    } catch (IOException ioe) {
//                        System.out.println(ioe.getMessage());
//                        System.out.println("Problem serializing node");
//                    }
                    break;
                }
                case "getfilesunderpath":
                    GetFilesUnderPath(m.ReadString(), outputToClient);
                    break;
                case "cd":
                case "dir":
                    GetValidityOfPath(m.ReadString(), outputToClient);
                    break;
                case "logicalfilecount":
                    LogicalFileCount(m.ReadString(), outputToClient);
                    break;

            }
            System.out.println("Finished client input");
            outputToClient.WriteDebugStatement("Finished client input");
            return outputToClient;
        }

        public void AssignChunkServerToFile(String fileName) {
        //make a random chunk server the location of this new file

            /*FileNode file = GetAtPath(fileName);
             if (file == null) {
             System.out.println("File does not exist; unable to assign chunk server to file");
             }
             */
            FileNode node = GetAtPath(fileName);
            if (!mChunkServers.isEmpty()) {
                Random newRandom = new Random();
                MySocket chunkServerSocket = mChunkServers.get(newRandom.nextInt(Integer.MAX_VALUE) % mChunkServers.size());
                node.AddChunkAtLocation(chunkServerSocket.GetID());
                Message newChunkMessage = new Message();
                newChunkMessage.WriteString("sm-makenewfile");
                newChunkMessage.WriteString(fileName);
                newChunkMessage.SetSocket(chunkServerSocket);
                mPendingMessages.push(newChunkMessage);
                for (int i = 0; i < (mChunkServers.size() - 1 < NUM_REPLICAS ? mChunkServers.size() - 1 : NUM_REPLICAS); ++i) {
                    MySocket newChunkServerSocket = mChunkServers.get(newRandom.nextInt(Integer.MAX_VALUE) % mChunkServers.size());
                    while (node.DoesChunkExistAtLocation(0, newChunkServerSocket.GetID())) {
                        newChunkServerSocket = mChunkServers.get(newRandom.nextInt(Integer.MAX_VALUE) % mChunkServers.size());
                    }
                    System.out.println("Adding replica at server: " + newChunkServerSocket.GetID());
                    node.AddReplicaAtLocation(0, newChunkServerSocket.GetID());
                }
            } else {
                System.out.println("No Chunk servers connected to server");
            }

        }

        public void GetValidityOfPath(String path, Message m) {
            FileNode nodeAtPath = GetAtPath(path);
            m.WriteString("SM-TestPathResponse");
            if (nodeAtPath == null) {
                m.WriteInt(0);
                m.WriteDebugStatement("Path " + path + " does not exist");
            } else {
                m.WriteInt(1);
            }
        }

        /**
         * Retrieves and returns a file node specified in the parameter
         *
         * @param filePath path to file
         * @return file node
         */
        public void GetFilesUnderPath(String path, Message m) {
            FileNode topNode = GetAtPath(path);
            if (topNode == null) {
                System.out.println("Path does not exist");
                m.WriteDebugStatement("Path does not exist");
                return;
            }
            ArrayList<String> totalPath = new ArrayList<>();
            m.WriteString("SM-GetFilesUnderPathResponse");
            totalPath.add(path);
            for (FileNode fn : topNode.mChildren) {
                RecurseGetFilesUnderPath(fn, totalPath, path, m);
            }
            m.WriteInt(totalPath.size());
            for (String s : totalPath) {
                m.WriteString(s);
            }
        }

        public void RemoveLocationFromFiles(String ReplicaInfo) {
            RecurseRemoveLocationFromFiles(mFileRoot, ReplicaInfo);
        }

        public void RecurseRemoveLocationFromFiles(FileNode curNode, String ReplicaInfo) {
            if (curNode.mIsDirectory) {
                for (FileNode fn : curNode.mChildren) {
                    RecurseRemoveLocationFromFiles(fn, ReplicaInfo);
                }
            } else {
                curNode.GetChunkDataAtIndex(0).RemoveChunkLocation(ReplicaInfo);
            }
        }

        public void RecurseGetFilesUnderPath(FileNode curNode, ArrayList<String> totalPaths, String parentPath, Message m) {
            if (curNode.mIsDirectory) {
                totalPaths.add(parentPath + "/" + curNode.mName);
                System.out.println(parentPath + "/" + curNode.mName);
                for (FileNode fn : curNode.mChildren) {
                    RecurseGetFilesUnderPath(fn, totalPaths, parentPath + "/" + curNode.mName, m);
                }
            }

        }

        public FileNode GetAtPath(String filePath) {
            // check for the initial "/"
            if (filePath.indexOf("/") != 0) {
                filePath = "/" + filePath;
            }
            String[] filePathTokens = filePath.split("/");
            FileNode curFile = mMaster.mFileRoot;
            // iterate through each directory in the path
            for (int i = 1; i < filePathTokens.length; ++i) {
                String dir = filePathTokens[i];
                boolean dirExists = false;
                if (!curFile.mIsDirectory) {
                    return null;
                }
                // if a match is found, set current file to the match
                for (FileNode file : curFile.mChildren) {
                    if (file.mName.equalsIgnoreCase(dir)) {
                        curFile = file;
                        dirExists = true;
                        break;
                    }
                }
                if (!dirExists) {
                    return null;
                }
            }
            // return the successfully retrieved file node
            return curFile;
        }

        /**
         * Creates a new unique directory in given pathname
         *
         * @param name path and name to create the new directory
         */
        public void CreateNewDir(String name, Message m) {
            // check for the initial "/"
            int firstIndex = name.indexOf("/");
            if (firstIndex != 0) {
                name = "/" + name;
            }
            // check if the given directory already exists
            if (GetAtPath(name) != null) {
                System.out.println("Directory already exists");
                m.WriteDebugStatement("Directory already exists");
                return;
            }
            // retrieve index of the last "/"
            int lastIndex = name.lastIndexOf("/");
            if (lastIndex < 0) {
                System.out.println("Invalid name");
                m.WriteDebugStatement("Invalid name");
                return;
            }
            // default parent node to the root node
            FileNode parentNode = mMaster.mFileRoot;
            // set parent node to the parent directory
            if (lastIndex > 1) {
                String parent = name.substring(0, lastIndex);
                parentNode = GetAtPath(parent);
                if (parentNode == null) {
                    System.out.println("Parent directory does not exist");
                    m.WriteDebugStatement("Parent directory does not exist");
                    return;
                }
            }
            // check for locks in the parent node
            if (parentNode.RequestWriteLock()) {
                try {
                    // save file structure
                    SaveFileStructure(true, name);
                    // create new directory
                    System.out.println("Creating new dir " + name);
                    m.WriteDebugStatement("Creating new dir " + name);
                    FileNode newDir = new FileNode(false);
                    newDir.mIsDirectory = true;
                    newDir.mName = name.substring(lastIndex + 1, name.length());
                    parentNode.mChildren.add(newDir);
                    System.out.println("Finished creating new dir");
                    m.WriteDebugStatement("Finished creating new dir");
                } finally {
                    parentNode.RequestWriteLock();
                }
            } else {
                System.out.println("Parent directory is locked, cancelling command");
                m.WriteDebugStatement("Parent directory is locked, cancelling command");
            }
        }

        /**
         * Creates the metadata for a new unique file at the given path
         *
         * @param name name of the new file
         */
        public void CreateNewFile(String name, Message m) {
            // check for the initial "/"
            int firstIndex = name.indexOf("/");
            if (firstIndex != 0) {
                name = "/" + name;
            }
            // check if the given file already exists
            if (GetAtPath(name) != null) {
                System.out.println("File already exists");
                m.WriteDebugStatement("File already exists");
                return;
            }
            // retrieve index of the last "/"
            int lastIndex = name.lastIndexOf("/");
            if (lastIndex < 0) {
                System.out.println("Invalid name");
                m.WriteDebugStatement("Invalid name");
                return;
            }
            // default parent node to the root node
            FileNode parentNode = mMaster.mFileRoot;
            // set parent node to the parent directory
            if (lastIndex > 1) {
                String parent = name.substring(0, lastIndex);
                parentNode = GetAtPath(parent);
                if (parentNode == null) {
                    System.out.println("Parent directory does not exist");
                    m.WriteDebugStatement("Parent directory does not exist");
                    return;
                }
            }
            // verify if the parent node is a directory
            if (!parentNode.mIsDirectory) {
                System.out.println("Parent is not a directory");
                m.WriteDebugStatement("Parent is not a directory");
                return;
            }
            // check for locks in the parent node
            if (parentNode.RequestWriteLock()) {
                try {
                    // save file structure
                    SaveFileStructure(false, name);
                    // create new file
                    System.out.println("Creating new file " + name);
                    m.WriteDebugStatement("Creating new file " + name);
                    FileNode newFile = new FileNode(true);
                    newFile.mIsDirectory = false;
                    newFile.mName = name.substring(lastIndex + 1, name.length());
                    parentNode.mChildren.add(newFile);
                    System.out.println("Finished creating new dir");
                    m.WriteDebugStatement("Finished creating new dir");
                    //make a random chunk server the location of this new file
                    AssignChunkServerToFile(name);
                } finally {
                    parentNode.ReleaseWriteLock();
                }
            } else {
                System.out.println("Parent directory is locked, cancelling command");
                m.WriteDebugStatement("Parent directory is locked, cancelling command");
            }
        }

        public void SeekFile(String fileName, int offset, int length, Message output) {
            FileNode fileNode = GetAtPath(fileName);
            if (fileNode == null) {
                System.out.println("File does not exist");
                output.WriteDebugStatement("File does not exist");
                return;
            }
            if (fileNode.RequestReadLock()) {
                try {
                    fileName = fileName.replaceAll("/", ".");
                    Path filePath = Paths.get(fileName);
                    BufferedInputStream br = new BufferedInputStream(Files.newInputStream(filePath, READ));

                    byte[] data = new byte[length];
                    if (br.read(data, offset, length) > 0) {
                        output.WriteString("SM-SeekFileResponse");
                        output.WriteString(fileName);
                        output.WriteInt(data.length);
                        output.AppendData(data);
                    } else {
                        output.WriteDebugStatement("Failed to read given length from offset");
                    }
                } catch (IOException ie) {
                    output.WriteDebugStatement("Unable to read file");
                } finally {
                    fileNode.ReleaseReadLock();
                }
            } else {
                System.out.println("File is locked, cancelling seek command");
                output.WriteDebugStatement("File is locked, cancelling seek command");
            }
        }

        public void ReadFile(String fileName, Message output) {
            System.out.println("Sending back info for readfile");
            FileNode fileNode = GetAtPath(fileName);
            if (fileNode == null) {
                System.out.println("File does not exist");
                output.WriteDebugStatement("File does not exist");
                return;
            }
            if(fileNode.RequestReadLock()){
                try {
                    FileNode.ChunkMetadata chunk = fileNode.GetChunkDataAtIndex(0);
                    output.WriteString("sm-readfileresponse");
                    output.WriteString(fileName);
                    output.WriteString(chunk.GetPrimaryLocation());
                    output.WriteInt(chunk.GetReplicaLocations().size());
                    for (String s : chunk.GetReplicaLocations()) {
                        output.WriteString(s);
                    }
                } finally {
                    fileNode.ReleaseWriteLock();
                }
            } else {
                System.out.println("File is locked, cancelling read command");
                output.WriteDebugStatement("File is locked, cancelling read command");
            } 
        }

        public void ReadFile(String fileName, String clientfileName, Message output) {
            FileNode fileNode = GetAtPath(fileName);
            if (fileNode == null) {
                System.out.println("File does not exist");
                output.WriteDebugStatement("File does not exist");
                return;
            }
            if (fileNode.RequestReadLock()) {
                try {
                    fileName = fileName.replaceAll("/", ".");
                    Path filePath = Paths.get(fileName);
                    byte[] data = Files.readAllBytes(filePath);
                    output.WriteString("SM-ReadFileResponse");
                    output.WriteString(clientfileName);
                    output.WriteInt(data.length);
                    output.AppendData(data);
                } catch (IOException ioe) {
                    output.WriteDebugStatement("Cannot open file to read");
                    output.WriteDebugStatement(ioe.getMessage());
                } finally {
                    fileNode.ReleaseReadLock();
                }
            }
            else {
                System.out.println("File is locked, cancelling read command");
                output.WriteDebugStatement("File is locked, cancelling read command");
            }
        }

        public void LogicalFileCount(String fileName, Message output) {
            FileNode file = GetAtPath(fileName);
            if (file == null) {
                output.WriteDebugStatement("File " + fileName + " does not exist");
                return;
            }
            if (file.RequestReadLock()) {
                try {
                    fileName = fileName.replaceAll("/", ".");
                    Path filePath = Paths.get(fileName);
                    File f = new File(fileName);
                    if (f.exists()) {
                        BufferedInputStream in = new BufferedInputStream(Files.newInputStream(filePath));
                        int numFiles = 0;
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
                            numFiles++;
                        }
                        output.WriteString("SM-LogicalFileCountResponse");
                        output.WriteInt(numFiles);
                    } else {
                        output.WriteDebugStatement("File " + fileName + " does not exist");
                    }
                } catch (IOException ioe) {
                    System.out.println(ioe.getMessage());
                    ioe.printStackTrace();
                } finally {
                    file.ReleaseReadLock();
                }
            } else {
                System.out.println("File is locked, cancelling count command");
                output.WriteDebugStatement("File is locked, cancelling count command");
            }
        }

        public void AppendFile(String fileName, Message output) {
            FileNode file = GetAtPath(fileName);
            if (file == null) {
                CreateNewFile(fileName, output);
            }
            if (file.RequestWriteLock()) {
                try {
                    //TODO error check if the file.GetChunkDataAtIndex return null
                    FileNode.ChunkMetadata chunk = file.GetChunkDataAtIndex(0);
                    output.WriteString("sm-appendresponse");
                    output.WriteString(fileName);
                    output.WriteString(chunk.GetPrimaryLocation());
                    output.WriteInt(chunk.GetReplicaLocations().size());
                    for (String s : chunk.GetReplicaLocations()) {
                        output.WriteString(s);
                    }

                    Message toChunkServer = new Message();
                    toChunkServer.WriteString("sm-makeprimary");
                    toChunkServer.WriteString(fileName);
                    toChunkServer.SetSocket(GetSocket(chunk.GetPrimaryLocation()));
                    //toChunkServer.Send();
                    mPendingMessages.push(toChunkServer);
                } finally {
                    file.ReleaseWriteLock();
                }
            } else {
                System.out.println("File is locked, cancelling append command");
                output.WriteDebugStatement("File is locked, cancelling append command");
            }
        }

        public void AppendFile(String fileName, byte[] data, Message output) {
            FileNode file = GetAtPath(fileName);
            if (file == null) {
                CreateNewFile(fileName, output);
            }
            if (file.RequestWriteLock()) {
                try {
                    fileName = fileName.replaceAll("/", ".");
                    Path filePath = Paths.get(fileName);
                    File f = new File(fileName);
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
                    }
                    OutputStream out = new BufferedOutputStream(Files.newOutputStream(filePath, CREATE, APPEND));
                    out.write(ByteBuffer.allocate(4).putInt(data.length).array());
                    out.write(data);
                    out.close();
                    System.out.println("Finished writing file");
                    output.WriteDebugStatement("Finished writing file");

                } catch (IOException ioe) {
                    System.out.println(ioe.getMessage());
                    ioe.printStackTrace();
                } finally {
                    file.ReleaseWriteLock();
                }
            } else {
                System.out.println("File is locked, cancelling append command");
                output.WriteDebugStatement("File is locked, cancelling append command");
            }
        }

        public void WriteFile(String fileName, byte[] data, Message output) {

            FileNode file = GetAtPath(fileName);
            if (file != null) {
                System.out.println("File already exists: " + fileName);
                output.WriteDebugStatement("File already exists: " + fileName);
                return;
            }
            if (file.RequestWriteLock()){
                CreateNewFile(fileName, output);
                file = GetAtPath(fileName);
                Message toChunkServer = new Message();
                MySocket chunkServerSocket = GetSocket(file.GetChunkLocationAtIndex(0));
                toChunkServer.SetSocket(chunkServerSocket);
                toChunkServer.WriteDebugStatement("Making primary");
                toChunkServer.WriteString("sm-makeprimary");
                mPendingMessages.push(toChunkServer);
                try {
                    fileName = fileName.replaceAll("/", ".");
                    Path filePath = Paths.get(fileName);
                    OutputStream out = new BufferedOutputStream(Files.newOutputStream(filePath, CREATE, APPEND));
                    out.write(data);
                    out.close();
                    System.out.println("Finished appending to file");
                    output.WriteDebugStatement("Finished appending to file");

                } catch (IOException ioe) {
                    System.out.println(ioe.getMessage());
                    ioe.printStackTrace();
                } finally {
                    file.ReleaseWriteLock();
                }
            } else {
                System.out.println("File is locked, cancelling write command");
                output.WriteDebugStatement("File is locked, cancelling write command");
            }
        }

        public void DeleteDirectory(String path, Message m) {
            // check for the first "/"
            int firstIndex = path.indexOf("/");
            if (firstIndex != 0) {
                path = "/" + path;
            }
            // check if the path is the root
            if (path.equals("/")) {
                System.out.println("Cannot delete root folder");
                m.WriteDebugStatement("Cannot delete root folder");
                return;
            }
            // check if the given path exists
            FileNode file = GetAtPath(path);
            if (file == null) {
                System.out.println("Path does not exist");
                m.WriteDebugStatement("Path does not exist");
                return;
            }
            // retrieve index of the last "/"
            int lastIndex = path.lastIndexOf("/");
            if (lastIndex < 0) {
                System.out.println("Invalid name");
                m.WriteDebugStatement("Invalid name");
                return;
            }
            // default parent node to the root node
            FileNode parentNode = mMaster.mFileRoot;
            // set parent node to the parent directory
            if (lastIndex > 1) {
                String parent = path.substring(0, lastIndex);
                parentNode = GetAtPath(parent);
                if (parentNode == null) {
                    System.out.println("Parent directory does not exist");
                    m.WriteDebugStatement("Parent directory does not exist");
                    return;
                }
            }
            // check for locks in the parent node
            if (parentNode.RequestWriteLock()) {
                try {
                    if (file.RequestWriteLock()) {
                        try {
                            // check if the path is a file
                            if (!file.mIsDirectory) {
                                DeleteFile(path, m);
                                return;
                            }
                            ArrayList<String> allFiles = new ArrayList<String>();
                            ArrayList<String> allDirs = new ArrayList<String>();
                            allDirs.add(path);
                            // find all children in the directory
                            Boolean success = true;
                            for (FileNode child : file.mChildren) {
                                if (child.mIsDirectory) {
                                    allDirs.add(path + "/" + child.mName);
                                    success =RecursiveDeleteDirectory(path + "/" + child.mName, child, allDirs, allFiles, m);
                                } else {
                                    allFiles.add(path + "/" + child.mName);
                                    success = true;
                                }
                            }
                            if(!success) {
                                System.out.println("Directory is in use, cancelling delete");
                                m.WriteDebugStatement("Directory is in use, cancelling delete");
                            } else {
                                // delete files from file structure
                                for (String fileName : allFiles) {
                                    DeleteFile(fileName, m);
                                }
                                // delete directories from file structure
                                for (String dir : allDirs) {
                                    DeleteFromFileStructure(true, dir);
                                    // delete directory
                                    System.out.println("Deleting directory " + dir);
                                    m.WriteDebugStatement("Deleting directory " + dir);
                                }
                                // remove link from parent node to this directory
                                parentNode.mChildren.remove(file);
                            }
                        } finally {
                            file.ReleaseWriteLock();
                        }
                    } else {
                        System.out.println("File is currently in use, cancelling command");
                        m.WriteDebugStatement("File is currently in use, cancelling command");
                    }
                } finally {
                    parentNode.ReleaseWriteLock();
                }
            } else {
                System.out.println("Parent directory is locked, cancelling command");
                m.WriteDebugStatement("Parent directory is locked, cancelling command");
            }
        }

        public Boolean RecursiveDeleteDirectory(String path, FileNode file, ArrayList<String> allDirs, ArrayList<String> allFiles, Message m) {
            // delete all children in the directory
            Boolean success;
            for (FileNode child : file.mChildren) {
                if (child.RequestWriteLock()) {
                    try {
                        if (child.mIsDirectory) {
                            allDirs.add(path + "/" + child.mName);
                            success = RecursiveDeleteDirectory(path + "/" + child.mName, child, allDirs, allFiles, m);
                        } else {
                            allFiles.add(path + "/" + child.mName);
                            success = true;
                        }
                    } finally {
                        child.ReleaseWriteLock();
                    }
                    if(!success)
                        return false;
                }
                else {
                    System.out.println("Child directory is locked, cancelling command");
                    m.WriteDebugStatement("Child directory is locked, cancelling command");
                    return false;
                }
            }
            return true;
        }

        /**
         * Removes a given file from the parent's list of children
         *
         * @param filePath path to the file to remove
         */
        public void DeleteFile(String filePath, Message m) {
            // retrieve file node
            FileNode file = GetAtPath(filePath);
            // retrieve index of the last "/"
            int lastIndex = filePath.lastIndexOf("/");
            // default parent node to the root node
            FileNode parentNode = mMaster.mFileRoot;
            // set parent node to the parent directory
            if (lastIndex > 1) {
                String parent = filePath.substring(0, lastIndex);
                parentNode = GetAtPath(parent);
            }
            // grab write lock on parent node
            if (parentNode.RequestWriteLock()) {
                // delete file from file structure
                DeleteFromFileStructure(false, filePath);
                // delete file
                System.out.println("Deleting file " + filePath);
                m.WriteDebugStatement("Deleting file " + filePath);
                parentNode.mChildren.remove(file);
                try {
                    Path path = FileSystems.getDefault().getPath(filePath);
                    Files.deleteIfExists(path);
                } catch (IOException ie) {
                    System.out.println("Could not delete physical file");
                } finally {
                    parentNode.ReleaseWriteLock();
                }
            } else {
                System.out.println("Parent directory is locked, cancelling delete command");
                m.WriteDebugStatement("Parent directory is locked, cancelling delete command");
            }
        }

        /**
         * Checks path and lists all children in the path
         *
         * @param filePath directory to search through
         */
        public void ListFiles(String filePath, Message m) {
            System.out.println("Listing files for path: " + filePath);
            m.WriteDebugStatement("Listing files for path: " + filePath);
            FileNode fileDir = GetAtPath(filePath);
            if (fileDir == null) {
                System.out.println("No directory named " + filePath + " exists");
                m.WriteDebugStatement("No directory named " + filePath + " exists");
                return;
            }
            if (!fileDir.mIsDirectory) {
                System.out.println("Path is not a directory");
                m.WriteDebugStatement("Path is not a directory");
                return;
            }
            if (fileDir.mChildren.isEmpty()) {
                System.out.println("No files in directory " + filePath);
                m.WriteDebugStatement("No files in directory " + filePath);
                return;
            }
            // check for locks in file directory
            if (fileDir.RequestReadLock()) {
                try {
                    for (FileNode file : fileDir.mChildren) {
                        System.out.println(file.mName);
                        m.WriteDebugStatement(file.mName);
                    }
                } finally {
                    fileDir.ReleaseReadLock();
                }
            } else {
                System.out.println("Directory is locked, cancelling command");
                m.WriteDebugStatement("Directory is locked, cancelling command");
            }
            
        }
    }
}
