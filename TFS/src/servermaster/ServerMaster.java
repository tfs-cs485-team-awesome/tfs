/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package servermaster;

import java.io.*;
import java.net.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import tfs.Message;
import tfs.MySocket;

/**
 *
 * @author laurencewong
 */
public class ServerMaster {

    FileNode mFileRoot;
    ServerSocket mListenSocket;
    ArrayList<MySocket> mClients;
    ArrayList<MySocket> mChunkServers;

    public void Init() {
        mFileRoot = new FileNode(false);
        mFileRoot.mName = "/";
        mClients = new ArrayList<MySocket>();
        mChunkServers = new ArrayList<MySocket>();
    }

    public ServerMaster(int inSocketNum) {
        Init();
        try {
            System.out.println("Starting server on port " + inSocketNum);
            mListenSocket = new ServerSocket(inSocketNum);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    public ServerMaster() {
        Init();
        try {
            mListenSocket = new ServerSocket(6789);
        } catch (Exception e) {
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
                //BufferedReader newConnectionReader = new BufferedReader(new InputStreamReader(newConnection.getInputStream()));
                //String connectionType = newConnectionReader.readLine();
                Message m = new Message(newConnection.ReadBytes());
                String connectionType = m.ReadString();
                switch (connectionType) {
                    case "Client":
                        synchronized (mClients) {
                            mClients.add(newConnection);
                            System.out.println("Adding new client");
                        }
                        break;
                    case "ChunkServer":
                        synchronized (mChunkServers) {
                            mChunkServers.add(newConnection);
                            System.out.println("Adding new chunkserver");
                        }
                        break;
                    default:
                        System.out.println("Server was told new connection of type: " + connectionType);
                        break;
                }

            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println(e.getLocalizedMessage());
        }
        System.out.println("Exiting server");
    }

    public class ServerMasterClientThread extends Thread {

        ServerMaster mMaster;
        int mClientNum;

        public ServerMasterClientThread(ServerMaster inMaster) {
            mMaster = inMaster;
            System.out.println("Creating client serving thread");
        }

        public void run() {
            while (true) {
                try {
                    synchronized (mClients) {
                        for (MySocket clientSocket : mMaster.mClients) {
                            
                            if (clientSocket.hasData()) {
                                Message messageReceived = new Message(clientSocket.ReadBytes());
                                Message messageSending = new Message();
                                messageSending = ParseClientInput(messageReceived);
                                clientSocket.WriteMessage(messageSending);
                            }
                        }
                    }
                    synchronized (mChunkServers) {
                        for (MySocket chunkSocket : mMaster.mChunkServers) {

                        }
                    }
                    this.sleep(100);
                } catch (Exception e) {
                    
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                    //e.printStackTrace();
                }

            }
        }

        /**
         * Parses the client's input into a command and a parameter
         * @param input should be in the format "CommandName Parameters"
         */
        public Message ParseClientInput(Message m) {
            Message outputToClient = new Message();

            String command = m.ReadString();
            System.out.println("Server Received: " + command);
            switch (command) {
                case "CreateNewDirectory":
                case "createnewdirectory":
                case "mkdir":
                    CreateNewDir(m.ReadString());
                    break;
                case "CreateNewFile":
                case "createnewfile":
                case "touch":
                    CreateNewFile(m.ReadString());
                    break;
                case "DeleteFile":
                case "deletefile":
                case "rm":
                    DeleteFile(m.ReadString());
                    break;
                case "ListFiles":
                case "listfiles":
                case "ls":
                    ListFiles(m.ReadString());
                    break;
                case "ReadFile":
                case "readfile":
                case "read":
                    ReadFile(m.ReadString(), m.ReadInt());
                    break;
                case "WriteFile":
                case "writefile":
                case "write":
                    WriteFile(m.ReadString(), m.ReadData(m.ReadInt()));
                    break;
            }
            System.out.println("Finished client input");
            return outputToClient;
        }

        /**
         * Retrieves and returns a file node specified in the parameter
         * @param filePath path to file
         * @return file node
         */
        public FileNode GetAtPath(String filePath) {
            // check for the initial "/"
            if(filePath.indexOf("/") != 0){
                filePath = "/" + filePath;
            }
            String[] filePathTokens = filePath.split("/");
            FileNode curFile = mMaster.mFileRoot;
            // iterate through each directory in the path
            for (int i = 1; i < filePathTokens.length; ++i) {
                String dir = filePathTokens[i];
                boolean dirExists = false;
                if(!curFile.mIsDirectory){
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
         * @param name path and name to create the new directory
         */
        public void CreateNewDir(String name) {
            // check for the initial "/"
            int firstIndex = name.indexOf("/");
            if(firstIndex != 0){
                name = "/" + name;
            }
            // check if the given directory already exists
            if (GetAtPath(name) != null) {
                System.out.println("Directory already exists");
                return;
            }
            // retrieve index of the last "/"
            int lastIndex = name.lastIndexOf("/");
            if (lastIndex < 0) {
                System.out.println("Invalid name");
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
                    return;
                }
            }
            // check for locks in the parent node
            if (parentNode.mReadLock || parentNode.mWriteLock) {
                System.out.println("Parent directory is locked, cancelling command");
                return;
            }
            // create new directory
            System.out.println("Creating new dir " + name);
            FileNode newDir = new FileNode(false);
            newDir.mIsDirectory = true;
            newDir.mName = name.substring(lastIndex + 1, name.length());
            parentNode.mChildren.add(newDir);
            System.out.println("Finished creating new dir");
            return;
        }
        
        /**
         * Creates the metadata for a new unique file at the given path
         * @param name name of the new file
         */
        public void CreateNewFile(String name) {
            // check for the initial "/"
            int firstIndex = name.indexOf("/");
            if(firstIndex != 0){
                name = "/" + name;
            }
            // check if the given file already exists
            if(GetAtPath(name) != null){
                System.out.println("File already exists");
                return;
            }
            // retrieve index of the last "/"
            int lastIndex = name.lastIndexOf("/");
            if(lastIndex < 0){
                System.out.println("Invalid name");
                return;
            }
            // default parent node to the root node
            FileNode parentNode = GetAtPath("/");
            // set parent node to the parent directory
            if(lastIndex > 1){
                String parent = name.substring(0, lastIndex);
                parentNode = GetAtPath(parent);
                if (parentNode == null) {
                    System.out.println("Parent directory does not exist");
                    return;
                }
            }
            // verify if the parent node is a directory
            if(!parentNode.mIsDirectory){
                System.out.println("Parent is not a directory");
                return;
            }
            // check for locks in the parent node
            if (parentNode.mReadLock || parentNode.mWriteLock) {
                System.out.println("Parent directory is locked, cancelling command");
                return;
            }
            // create new file
            System.out.println("Creating new file " + name);
            FileNode newFile = new FileNode(true);
            newFile.mIsDirectory = false;
            newFile.mName = name.substring(lastIndex+1,name.length());
            parentNode.mChildren.add(newFile);
            System.out.println("Finished creating new dir");
            return;
        }
                
        public void ReadFile(String fileName, int length){
            FileNode file = GetAtPath(fileName);
            if (file == null) {
                System.out.println("File does not exist");
                return;
            }
            
        }
        
        public void WriteFile(String fileName, byte[] data){
            
        }
        
        /**
         * Removes a given file from the parent's list of children
         * @param filePath path to the file to remove
         */
        public void DeleteFile(String filePath){
            // check for the first "/"
            int firstIndex = filePath.indexOf("/");
            if(firstIndex != 0){
                filePath = "/" + filePath;
            }
            // check if the given file exists
            FileNode file = GetAtPath(filePath);
            if(file == null){
                System.out.println("File does not exist");
                return;
            }
            // verify that the file is a file
            if(file.mIsDirectory){
                System.out.println("Deletion cancelled, " + filePath + " is a directory");
                return;
            }
            // retrieve index of the last "/"
            int lastIndex = filePath.lastIndexOf("/");
            if(lastIndex < 0){
                System.out.println("Invalid name");
                return;
            }
            // default parent node to the root node
            FileNode parentNode = GetAtPath("/");
            // set parent node to the parent directory
            if(lastIndex > 1){
                String parent = filePath.substring(0, lastIndex);
                parentNode = GetAtPath(parent);
                if (parentNode == null) {
                    System.out.println("Parent directory does not exist");
                    return;
                }
            }
            // check for locks in the parent node
            if (parentNode.mReadLock || parentNode.mWriteLock) {
                System.out.println("Parent directory is locked, cancelling command");
                return;
            }
            // check for locks on the file
            if (file.mReadLock || file.mWriteLock) {
                System.out.println("File is currently in use, cancelling command");
                return;
            }
            // delete file
            System.out.println("Deleting file " + filePath);
            parentNode.mChildren.remove(file);
            try {
                Path path = FileSystems.getDefault().getPath(filePath);
                Files.deleteIfExists(path);
            } catch (IOException ie) {
                System.out.println("Could not delete physical file");
            }
        }
        
        /**
         * Checks path and lists all children in the path
         * @param filePath directory to search through
         */
        public void ListFiles(String directory) {
            System.out.println("Listing files in directory: " + directory);
            FileNode fileDir = GetAtPath(directory);
            if (fileDir == null) {
                System.out.println("No directory named " + directory + " exists");
                return;
            }
            if(!fileDir.mIsDirectory){
                System.out.println("Path is not a directory");
                return;
            }
            if(fileDir.mChildren.isEmpty()){
                System.out.println("No files in directory " + directory);
                return;
            }
            // check for locks in file directory
            if (fileDir.mWriteLock) {
                System.out.println("Directory is locked, cancelling command");
                return;
            }
            for (FileNode file : fileDir.mChildren) {
                System.out.println(file.mName);
            }
        }
    }
}
