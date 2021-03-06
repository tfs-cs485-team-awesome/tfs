/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.servermaster;

import java.io.*;
import java.net.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import tfs.util.Callbackable;
import tfs.util.FileNode;
import tfs.util.HeartbeatSocket;
import tfs.util.Message;
import tfs.util.MySocket;

/**
 *
 * @author laurencewong
 */
public class ServerMaster implements Callbackable {
    
    FileNode mFileRoot;
    ServerSocket mListenSocket;
    HeartbeatSocket mHeartbeatSocket;
    final HashMap<String, ClientThread> mClients = new HashMap<>();
    final HashMap<String, MySocket> mChunkServers = new HashMap<>();
    ArrayDeque<Message> mPendingMessages; //messages to send out
    ArrayDeque<String> mSocketsToClose;

    final int NUM_REPLICAS = 3;

    /**
     * Returns IP Address
     * @return ip address
     * @throws IOException 
     */
    public final String GetIP() throws IOException {
        String ip = InetAddress.getLocalHost().toString();
        ip = ip.substring(ip.indexOf("/") + 1);
        return ip;
    }

    /**
     * Initialize the file system
     */
    public final void Init() {
        mFileRoot = new FileNode(false);
        mFileRoot.mName = "/";
        mPendingMessages = new ArrayDeque<>();
        mSocketsToClose = new ArrayDeque<>();
    }
    
    /**
     * Getter function for ChunkSocket ID
     * @param inSocketID the ChunkSocket ID
     * @return 
     */
    public MySocket GetChunkSocket(String inSocketID) {
        //most likely chunk socket, so search those first
        synchronized (mChunkServers) {
            return mChunkServers.get(inSocketID);
        }
    }

    /**
     * Constructor for ServerMaster
     * @param inSocketNum the socket number
     */
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

    /**
     * Default Constructor for ServerMaster
     */
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

    /**
     * Busy loop that checks for new clients and chunkservers
     */
    public void RunLoop() {
        System.out.println("Starting server");
        try {
            ServerMasterChunkServerThread newClientThread = new ServerMasterChunkServerThread(this);
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
                        String newClientID = m.ReadString() + ":" + newConnection.GetSocket().getPort();
                        //System.out.println("ClientID = " + newClientID);
                        ClientThread newClient = new ClientThread(newConnection, this, newClientID, mFileRoot);
                        synchronized (mClients) {
                            mClients.put(newClientID, newClient);
                        }
                        newClient.start();
                        System.out.println("Adding new client");
                        break;
                    }
                    case "ChunkServer": {
                        synchronized (mChunkServers) {

                            System.out.println("Accepting chunk server" + newConnection.GetSocket().getLocalAddress() + ((InetSocketAddress) newConnection.GetSocket().getLocalSocketAddress()).getPort());
                            Message sendID = new Message();
                            sendID.WriteString("setid");
			    String ChunkServerIP = m.ReadString();
                            int ChunkServerListenPort = m.ReadInt();
                            newConnection.SetID(ChunkServerIP + ":" + ChunkServerListenPort);
                            sendID.WriteString(newConnection.GetID());
                            System.out.println("Adding new chunkserver with ID : " + newConnection.GetID());
                            newConnection.WriteMessage(sendID);
                            mChunkServers.put(newConnection.GetID(), newConnection);
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

    /**
     * Saves File Structure to a file
     * @param isDirectory directory boolean of filestructure
     * @param name the name of file structure
     */
    public synchronized void SaveFileStructure(Boolean isDirectory, String name) {
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

    /**
     * Find file at given path
     * @param filePath the path to look for file
     * @return 
     */
    public FileNode GetAtPath(String filePath) {
        // check for the initial "/"
        if (filePath.indexOf("/") != 0) {
            filePath = "/" + filePath;
        }
        String[] filePathTokens = filePath.split("/");
        FileNode curFile = mFileRoot;
        // iterate through each directory in the path
        for (int i = 1; i < filePathTokens.length; ++i) {
            String dir = filePathTokens[i];
            boolean dirExists = false;
            if (!curFile.mIsDirectory) {
                return null;
            }
            // if a match is found, set current file to the match
            //curFile.mReadLock = true;
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
     * Removes server information from FileNode
     * @param ReplicaInfo the server information
     */
    public void RemoveLocationFromFiles(String ReplicaInfo) {
        RecurseRemoveLocationFromFiles(mFileRoot, ReplicaInfo);
    }

    /**
     * Recursively removes server information from FileNode and child nodes
     * @param curNode the current node 
     * @param ReplicaInfo the server information
     */
    public void RecurseRemoveLocationFromFiles(FileNode curNode, String ReplicaInfo) {
        if (curNode.mIsDirectory) {
            for (FileNode fn : curNode.mChildren) {
                RecurseRemoveLocationFromFiles(fn, ReplicaInfo);
            }
        } else {

            curNode.GetChunkDataAtIndex(0).RemoveChunkLocation(ReplicaInfo);
        }
    }

    /**
     * Assigns Chunk Server to File
     * @param fileName the file name
     * @param numReplicas the number of replicas file should have
     */
    public void AssignChunkServerToFile(String fileName, int numReplicas) {
        //make a random chunk server the location of this new file
        FileNode node = GetAtPath(fileName);
        synchronized (mChunkServers) {
            if (!mChunkServers.isEmpty()) {
                Random newRandom = new Random();
                List<MySocket> mChunkServerList = new ArrayList<>(mChunkServers.values());
                MySocket chunkServerSocket = mChunkServerList.get(newRandom.nextInt(Integer.MAX_VALUE) % mChunkServers.size());
                node.AddChunkAtLocation(chunkServerSocket.GetID());
                Message newChunkMessage = new Message();
                newChunkMessage.WriteString("sm-makenewfile");
                newChunkMessage.WriteString(fileName);
                newChunkMessage.SetSocket(chunkServerSocket);
                System.out.println("Pushing sm-makenewfile to mPendingMessages");
                mPendingMessages.push(newChunkMessage);
                for (int i = 0; i < (mChunkServers.size() - 1 < numReplicas ? mChunkServers.size() - 1 : numReplicas); ++i) {
                    MySocket newChunkServerSocket = mChunkServerList.get(newRandom.nextInt(Integer.MAX_VALUE) % mChunkServers.size());
                    while (node.DoesChunkExistAtLocation(0, newChunkServerSocket.GetID())) {
                        newChunkServerSocket = mChunkServerList.get(newRandom.nextInt(Integer.MAX_VALUE) % mChunkServers.size());
                    }
                    System.out.println("Adding replica at server: " + newChunkServerSocket.GetID());
                    node.AddReplicaAtLocation(0, newChunkServerSocket.GetID());
                }
            } else {
                System.out.println("No Chunk servers connected to server");
            }
        }
    }

    /**
     * Implements Callback function to close sockets
     * @param inParameter socket to close
     */
    @Override
    public void Callback(String inParameter) {
        synchronized (mSocketsToClose) {
            mSocketsToClose.push(inParameter);

        }
    }

    /**
     * Thread that handles chunk server requests on the master server
     */
    public class ServerMasterChunkServerThread extends Thread {

        ServerMaster mMaster;
        int mClientNum;

        /**
         * Constructor for ServerMasterChunkServerThread
         * @param inMaster the ServerMaster
         */
        public ServerMasterChunkServerThread(ServerMaster inMaster) {
            mMaster = inMaster;
            System.out.println("Creating chunk serving thread");
        }

        /**
         * Reads in messages from sockets and performs appropriate actions
         */
        @Override
        public void run() {
            LoadFileStructure();
            while (true) {
                try {
                    synchronized (mChunkServers) {
                        for (MySocket chunkSocket : mChunkServers.values()) {
                            if (chunkSocket.hasData()) {
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
                            if (mClients.containsKey(socketID)) {
                                mClients.get(socketID).Close();
                            } else if (mChunkServers.containsKey(socketID)) {
                                RemoveLocationFromFiles(socketID);
                                mChunkServers.get(socketID).close();
                                mChunkServers.remove(socketID);
                            } else {
                                System.out.println("Unable to find socket: " + socketID);
                            }
                        }
                    }

                    this.sleep(100);
                } catch (Exception e) {

                    System.out.println(e.getMessage());
                }

            }
        }

	/**
	 * Loads the file structure from the SYSTEM_LOG.txt file
	 */
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

	/**
	 * Deletes a file or directory from the SYSTEM_LOG.txt file
	 * @param isDirectory If the thing being deleted is a directory a file
	 * @param name  The name of the file/directory to delete
	 */
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

                String line;
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

	/**
	 * Creates a new directory using a name given by the SYSTEM_LOG.txt file
	 * @param name The name of the directory to create
	 */
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

	/**
	 * Creates a new file using a name given by the SYSTEM_LOG.txt file
	 * @param name The name of the file to create
	 */
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
        }
	
	/**
	 * Parses the input from chunk servers
	 * @param m The message from the chunk server
	 * @return The reply message
	 */
        public Message ParseChunkInput(Message m) {
            Message outputToChunk = new Message();

            String command = m.ReadString();
            System.out.println("Server receivd: " + command);
            switch (command.toLowerCase()) {
                case "chunksihave":
                    UpdateChunkLocations(m, outputToChunk);
                    break;
                case "appendtimestamp":
                    UpdateChunkTimestamp(m, outputToChunk);
                    break;
            }
            return outputToChunk;
        }

	/**
	 * Updates the location of a chunk
	 * @param m The message that contains the information of the chunk
	 * @param output The message to write output to
	 */
        public void UpdateChunkLocations(Message m, Message output) {
            String inChunkServerLocation = m.ReadString();
            int numChunks = m.ReadInt();
            for (int i = 0; i < numChunks; ++i) {
                String readPath = m.ReadString();
                long lastUpdated = m.ReadLong();
                FileNode fn = GetAtPath(readPath);
                if (fn == null) {
                    output.WriteDebugStatement("File " + readPath + " does not exist on the server");
                } else if (fn.mIsDirectory) {
                    output.WriteDebugStatement("File " + readPath + " is a directory on the server");
                } else {
                    if (fn.GetChunkDataAtIndex(0) == null) {
                        System.out.println(inChunkServerLocation);
                        fn.AddChunkAtLocation(inChunkServerLocation);
                        fn.GetChunkDataAtIndex(0).UpdateTimestamp(lastUpdated);
                    } else {
                        fn.GetChunkDataAtIndex(0).SetChunkLocation(inChunkServerLocation);
                        if (fn.GetChunkDataAtIndex(0).GetTimestamp() > lastUpdated) {
                            output.WriteString("sm-outofdatechunk");
                            output.WriteString(readPath.replaceAll("/", "\\."));
                            output.WriteString(fn.GetChunkDataAtIndex(0).GetPrimaryLocation());
                        }
                    }
                }
            }
        }

	/**
	 * Updates the timestamp of a chunk
	 * @param m The message that contains the name of the file and the timestamp
	 * @param output The message output 
	 */
        public void UpdateChunkTimestamp(Message m, Message output) {
            String filename = m.ReadString();
            long timestamp = m.ReadLong();
            filename = filename.replaceAll("\\.", "/");
            FileNode fileNode = GetAtPath(filename);
            if (fileNode == null) {
                System.out.println("Tried to update file: " + filename + " timestamp, but file does not exist");
                return;
            }
            if (fileNode.GetChunkDataAtIndex(0).GetTimestamp() > timestamp) {
                output.WriteString("sm-outofdatechunk");
                output.WriteString(filename.replaceAll("/", "\\."));
                output.WriteString(fileNode.GetChunkDataAtIndex(0).GetPrimaryLocation());
            } else {
                fileNode.GetChunkDataAtIndex(0).UpdateTimestamp(timestamp);
            }
        }
    }
}
