/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.servermaster;

import java.io.*;
import java.net.*;
import static java.nio.file.StandardOpenOption.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import tfs.util.Message;
import tfs.util.FileNode;
import tfs.util.MySocket;

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
            LoadFileStructure();
            while (true) {
                try {
                    synchronized (mClients) {
                        for (MySocket clientSocket : mMaster.mClients) {

                            if (clientSocket.hasData()) {
                                Message messageReceived = new Message(clientSocket.ReadBytes());
                                Message messageSending = ParseClientInput(messageReceived);
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
                }

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
            if (parentNode.mReadLock || parentNode.mWriteLock) {
                return;
            }
            FileNode newDir = new FileNode(false);
            newDir.mIsDirectory = true;
            newDir.mName = name.substring(lastIndex + 1, name.length());
            parentNode.mChildren.add(newDir);
            return;
        }

        public void CreateNewSetupFile(String name) {
            // retrieve index of the last "/"
            int lastIndex = name.lastIndexOf("/");
            if (lastIndex < 0) {
                return;
            }
            // default parent node to the root node
            FileNode parentNode = GetAtPath("/");
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
            switch (command) {
                case "CreateNewDirectory":
                case "createnewdirectory":
                case "mkdir":
                    CreateNewDir(m.ReadString(), outputToClient);
                    break;
                case "CreateNewFile":
                case "createnewfile":
                case "touch":
                    CreateNewFile(m.ReadString(), outputToClient);
                    break;
                case "DeleteFile":
                case "deletefile":
                case "rm":
                    DeleteFile(m.ReadString(), outputToClient);
                    break;
                case "ListFiles":
                case "listfiles":
                case "ls":
                    ListFiles(m.ReadString(), outputToClient);
                    break;
                case "ReadFile":
                case "readfile":
                case "read":
                    ReadFile(m.ReadString(), m.ReadInt(), m.ReadInt(), outputToClient);
                    break;
                case "WriteFile":
                case "writefile":
                case "write": {
                    String name = m.ReadString();
                    int lengthToRead = m.ReadInt();
                    byte[] data = m.ReadData(lengthToRead);
                    WriteFile(name, data, outputToClient);
                    break;
                }
                case "GetNode": {
                    String path = m.ReadString();
                    System.out.println("Getting node " + path);
                    outputToClient.WriteDebugStatement("Getting node " + path);
                    FileNode toClient = GetAtPath(path);
                    try {
                        outputToClient.WriteString("GetNodeResponse");
                        toClient.WriteToMessage(outputToClient);
                    } catch (IOException ioe) {
                        System.out.println(ioe.getMessage());
                        System.out.println("Problem serializing node");
                    }
                    break;
                }
                case "GetFilesUnderPath":
                    GetFilesUnderPath(m.ReadString(), outputToClient);
                    break;
            }
            System.out.println("Finished client input");
            outputToClient.WriteDebugStatement("Finished client input");
            return outputToClient;
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
            m.WriteString("GetFilesUnderPathResponse");
            RecurseGetFilesUnderPath(topNode, totalPath, "", m);
            m.WriteInt(totalPath.size());
            for (String s : totalPath) {
                m.WriteString(s);
            }
        }

        public void RecurseGetFilesUnderPath(FileNode curNode, ArrayList<String> totalPaths, String parentPath, Message m) {
            if (curNode.mIsDirectory) {/*
                if (curNode.mName.contentEquals("/")) {
                    totalPaths.add("/");
                } else{
                    if(parentPath.contentEquals("/")) {
                        totalPaths.add("/" + curNode.mName);
                    }
                    else {*/
                        totalPaths.add(parentPath + "/" + curNode.mName);
                   // }
                //}

                for (FileNode fn : curNode.mChildren) {
                    RecurseGetFilesUnderPath(fn, totalPaths, parentPath + curNode.mName, m);
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
            if (parentNode.mReadLock || parentNode.mWriteLock) {
                System.out.println("Parent directory is locked, cancelling command");
                m.WriteDebugStatement("Parent directory is locked, cancelling command");
                return;
            }
            // create new directory
            System.out.println("Creating new dir " + name);
            m.WriteDebugStatement("Creating new dir " + name);
            FileNode newDir = new FileNode(false);
            newDir.mIsDirectory = true;
            newDir.mName = name.substring(lastIndex + 1, name.length());
            parentNode.mChildren.add(newDir);
            System.out.println("Finished creating new dir");
            m.WriteDebugStatement("Finished creating new dir");
            return;
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
            FileNode parentNode = GetAtPath("/");
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
            if (parentNode.mReadLock || parentNode.mWriteLock) {
                System.out.println("Parent directory is locked, cancelling command");
                m.WriteDebugStatement("Parent directory is locked, cancelling command");
                return;
            }
            // create new file
            System.out.println("Creating new file " + name);
            m.WriteDebugStatement("Creating new file " + name);
            FileNode newFile = new FileNode(true);
            newFile.mIsDirectory = false;
            newFile.mName = name.substring(lastIndex + 1, name.length());
            parentNode.mChildren.add(newFile);
            System.out.println("Finished creating new dir");
            m.WriteDebugStatement("Finished creating new dir");
            return;
        }
                
        public void ReadFile(String fileName, int offset, int length, Message output){
            FileNode fileNode = GetAtPath(fileName);
            if (fileNode == null) {
                System.out.println("File does not exist");
                output.WriteDebugStatement("File does not exist");
                return;
            }
            try {
                Path filePath = Paths.get(fileName);
                BufferedInputStream br = new BufferedInputStream(Files.newInputStream(filePath, READ));

                byte[] data = new byte[length];
                if(br.read(data, offset, length) >= 0) {
                    output.WriteString("ReadFileResponse");
                    output.WriteString(fileName);
                    output.WriteInt(data.length);
                    output.AppendData(data);
                } else {
                    output.WriteDebugStatement("Could not read length and offset from file");
                }
            } catch (IOException ie) {
                output.WriteDebugStatement("Unable to read file");
            }
        }

        public void WriteFile(String fileName, byte[] data, Message output) {

            FileNode file = GetAtPath(fileName);
            if (file == null) {
                System.out.println("File does not exist");
                output.WriteDebugStatement("File does not exist");
                return;
            }

            try {
                Path filePath = Paths.get(fileName);
                OutputStream out = new BufferedOutputStream(Files.newOutputStream(filePath, CREATE, APPEND));
                out.write(data);
                out.close();
                System.out.println("Finished writing file");
                output.WriteDebugStatement("Finished writing file");

            } catch (IOException ioe) {
                System.out.println(ioe.getMessage());
                ioe.printStackTrace();
            }

        }

        /**
         * Removes a given file from the parent's list of children
         *
         * @param filePath path to the file to remove
         */
        public void DeleteFile(String filePath, Message m) {
            // check for the first "/"
            int firstIndex = filePath.indexOf("/");
            if (firstIndex != 0) {
                filePath = "/" + filePath;
            }
            // check if the given file exists
            FileNode file = GetAtPath(filePath);
            if (file == null) {
                System.out.println("File does not exist");
                m.WriteDebugStatement("File does not exist");
                return;
            }
            // verify that the file is a file
            if (file.mIsDirectory) {
                System.out.println("Deletion cancelled, " + filePath + " is a directory");
                m.WriteDebugStatement("Deletion cancelled, " + filePath + " is a directory");
                return;
            }
            // retrieve index of the last "/"
            int lastIndex = filePath.lastIndexOf("/");
            if (lastIndex < 0) {
                System.out.println("Invalid name");
                m.WriteDebugStatement("Invalid name");
                return;
            }
            // default parent node to the root node
            FileNode parentNode = GetAtPath("/");
            // set parent node to the parent directory
            if (lastIndex > 1) {
                String parent = filePath.substring(0, lastIndex);
                parentNode = GetAtPath(parent);
                if (parentNode == null) {
                    System.out.println("Parent directory does not exist");
                    m.WriteDebugStatement("Parent directory does not exist");
                    return;
                }
            }
            // check for locks in the parent node
            if (parentNode.mReadLock || parentNode.mWriteLock) {
                System.out.println("Parent directory is locked, cancelling command");
                m.WriteDebugStatement("Parent directory is locked, cancelling command");
                return;
            }
            // check for locks on the file
            if (file.mReadLock || file.mWriteLock) {
                System.out.println("File is currently in use, cancelling command");
                return;
            }
            // delete file
            System.out.println("Deleting file " + filePath);
            m.WriteDebugStatement("Deleting file " + filePath);
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
            if (fileDir.mWriteLock) {
                System.out.println("Directory is locked, cancelling command");
                m.WriteDebugStatement("Directory is locked, cancelling command");
                return;
            }
            for (FileNode file : fileDir.mChildren) {
                System.out.println(file.mName);
                m.WriteDebugStatement(file.mName);
            }
        }
    }
}
