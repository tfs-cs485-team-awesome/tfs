/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.servermaster;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import java.util.ArrayDeque;
import java.util.ArrayList;
import tfs.util.*;

/**
 *
 * @author laurencewong
 */
public class ClientThread extends Thread {

    private volatile boolean isRunning = true;

    MySocket mSocket;
    ArrayDeque<Message> mPendingMessages;
    FileNode mFileRoot;
    ServerMaster mMaster;

    public final void Init() {
        mPendingMessages = new ArrayDeque<>();
    }

    public final void SendID() {
        Message sendID = new Message();
        sendID.WriteString("setid");
        sendID.WriteString(mSocket.GetID());
        sendID.SetSocket(mSocket);
        mPendingMessages.push(sendID);
    }

    public final String GetID() {
        return mSocket.GetID();
    }

    public ClientThread(MySocket inSocket, ServerMaster inMaster, String inID, FileNode inNode) {
        mSocket = inSocket;
        mSocket.SetID(inID);
        mFileRoot = inNode;
        mMaster = inMaster;
        Init();
        SendID();
    }

    public void Close() throws IOException {
        mSocket.close();
        isRunning = false;
    }

    @Override
    public void run() {
        System.out.println("Running client thread");
        while (isRunning) {
            try {
                if (mSocket.hasData()) {
                    Message messageReceived = new Message(mSocket.ReadBytes());
                    Message messageSending = ParseClientInput(messageReceived);
                    messageSending.SetSocket(mSocket);
                    mPendingMessages.push(messageSending);
                }
                if (!mPendingMessages.isEmpty()) {
                    mPendingMessages.pop().Send();
                }
            } catch (IOException ioe) {
                System.out.println(ioe.getMessage());
            }
        }
    }

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

    public void RecurseGetFilesUnderPath(FileNode curNode, ArrayList<String> totalPaths, String parentPath, Message m) {
        if (curNode.mIsDirectory) {
            totalPaths.add(parentPath + "/" + curNode.mName);
            System.out.println(parentPath + "/" + curNode.mName);
            for (FileNode fn : curNode.mChildren) {
                RecurseGetFilesUnderPath(fn, totalPaths, parentPath + "/" + curNode.mName, m);
            }
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
                if (!success) {
                    return false;
                }
            } else {
                System.out.println("Child directory is locked, cancelling command");
                m.WriteDebugStatement("Child directory is locked, cancelling command");
                return false;
            }
        }
        return true;
    }

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

    public Message ParseClientInput(Message m) {
        Message outputToClient = new Message();

        String command = m.ReadString();
        System.out.println("Server Received: " + command);
        outputToClient.WriteDebugStatement("Server Received " + command);
        switch (command.toLowerCase()) {
            case "mkdir":
                CreateNewDir(m.ReadString(), outputToClient);
                break;
            case "touch":
                CreateNewFile(m.ReadString(), outputToClient);
                break;
            case "rm":
                DeleteDirectory(m.ReadString(), outputToClient);
                break;
            case "ls":
                ListFiles(m.ReadString(), outputToClient);
                break;
            case "read":
                ReadFile(m.ReadString(), outputToClient);
                break;
            case "write": {
                /*String name = m.ReadString();
                 int numReplicas = m.ReadInt();
                 int lengthToRead = m.ReadInt();
                 byte[] data = m.ReadData(lengthToRead);
                 WriteFile(name, numReplicas, data, outputToClient);*/
                AppendFile(m.ReadString(), m.ReadInt(), outputToClient);
                break;
            }
            case "stream":
                AppendFile(m.ReadString(), outputToClient);
                break;
            /*case "getnode": {
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
             */
            case "getfilesunderpath":
                GetFilesUnderPath(m.ReadString(), outputToClient);
                break;

        }
        System.out.println("Finished client input");
        outputToClient.WriteDebugStatement("Finished client input");
        return outputToClient;
    }

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
        FileNode parentNode = mFileRoot;
        // set parent node to the parent directory
        if (lastIndex > 1) {
            String parent = name.substring(0, lastIndex);
            parentNode = GetAtPath(parent);
            if (parentNode == null || !parentNode.mIsDirectory) {
                System.out.println("Parent directory does not exist");
                m.WriteDebugStatement("Parent directory does not exist");
                return;
            }
        }
        // check for locks in the parent node
        if (parentNode.RequestWriteLock()) {
            try {
                // save file structure
                mMaster.SaveFileStructure(true, name);
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

    public void CreateNewFile(String name, Message m) {
        CreateNewFile(name, 2, m);
    }

    public void CreateNewFile(String name, int numReplicas, Message m) {
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
        FileNode parentNode = mFileRoot;
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
        //make sure more chunk servers exist than the numReplicas requested
        synchronized (mMaster.mChunkServers) {
            if (mMaster.mChunkServers.size() - 1 < numReplicas) {
                m.WriteDebugStatement("Not enough chunk servers");
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
                mMaster.SaveFileStructure(false, name);
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
                mMaster.AssignChunkServerToFile(name, numReplicas);
            } finally {
                parentNode.ReleaseWriteLock();
            }
        } else {
            System.out.println("Parent directory is locked, cancelling command");
            m.WriteDebugStatement("Parent directory is locked, cancelling command");
        }
    }

    public void AppendFile(String fileName, Message output) {
        AppendFile(fileName, 3, output);
    }

    public void AppendFile(String fileName, int numReplicas, Message output) {
        FileNode file = GetAtPath(fileName);
        if (file == null) {
            CreateNewFile(fileName, numReplicas, output);
            file = GetAtPath(fileName);
            if (file == null) { // create new file failed for some reason
                return;
            }
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
                toChunkServer.SetSocket(mMaster.GetChunkSocket(chunk.GetPrimaryLocation()));
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

    public void WriteFile(String fileName, int numReplicas, byte[] data, Message output) {

        FileNode file = GetAtPath(fileName);
        if (file != null) {
            System.out.println("File already exists: " + fileName);
            output.WriteDebugStatement("File already exists: " + fileName);
            return;
        }
        CreateNewFile(fileName, numReplicas, output);
        file = GetAtPath(fileName);
        if (file.RequestWriteLock()) {
            try {
                Message toChunkServer = new Message();
                MySocket chunkServerSocket = mMaster.GetChunkSocket(file.GetChunkLocationAtIndex(0));
                toChunkServer.SetSocket(chunkServerSocket);
                toChunkServer.WriteDebugStatement("Making primary");
                toChunkServer.WriteString("sm-makeprimary");
                mPendingMessages.push(toChunkServer);
            } finally {
                file.ReleaseWriteLock();
            }
        } else {
            System.out.println("File is locked, cancelling write command");
            output.WriteDebugStatement("File is locked, cancelling write command");
        }
    }

    public void ReadFile(String fileName, Message output) {
        System.out.println("Sending back info for readfile: " + fileName);
        FileNode fileNode = GetAtPath(fileName);
        if (fileNode == null) {
            System.out.println("File does not exist");
            output.WriteDebugStatement("File does not exist");
        } else {
            //TODO error check if the file.GetChunkDataAtIndex return null
            FileNode.ChunkMetadata chunk = fileNode.GetChunkDataAtIndex(0);
            output.WriteString("sm-readfileresponse");
            output.WriteString(fileName);
            output.WriteString(chunk.GetPrimaryLocation());
            output.WriteInt(chunk.GetReplicaLocations().size());
            for (String s : chunk.GetReplicaLocations()) {
                output.WriteString(s);
            }
        }
    }

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
        FileNode parentNode = mFileRoot;
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
                                success = RecursiveDeleteDirectory(path + "/" + child.mName, child, allDirs, allFiles, m);
                            } else {
                                allFiles.add(path + "/" + child.mName);
                                success = true;
                            }
                        }
                        if (!success) {
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

    public void DeleteFile(String filePath, Message m) {
        // retrieve file node
        FileNode file = GetAtPath(filePath);
        // retrieve index of the last "/"
        int lastIndex = filePath.lastIndexOf("/");
        // default parent node to the root node
        FileNode parentNode = mFileRoot;
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
                FileNode.ChunkMetadata chunk = file.GetChunkDataAtIndex(0);
                Message toChunkServer = new Message();
                toChunkServer.WriteString("sm-deletefile");
                toChunkServer.WriteString(filePath);
                toChunkServer.SetSocket(mMaster.GetChunkSocket(chunk.GetPrimaryLocation()));
                mPendingMessages.push(toChunkServer);
                for (int i = 0; i < chunk.GetReplicaLocations().size(); i++) {
                    chunk = file.GetChunkDataAtIndex(0);
                    toChunkServer = new Message();
                    toChunkServer.WriteString("sm-deletefile");
                    toChunkServer.WriteString(filePath);
                    toChunkServer.SetSocket(mMaster.GetChunkSocket(chunk.GetReplicaLocations().get(i)));
                    mPendingMessages.push(toChunkServer);
                }
            } finally {
                parentNode.ReleaseWriteLock();
            }
        } else {
            System.out.println("Parent directory is locked, cancelling delete command");
            m.WriteDebugStatement("Parent directory is locked, cancelling delete command");
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

    public void LogicalFileCount(String fileName, Message output) {
        FileNode file = GetAtPath(fileName);
        if (file == null) {
            output.WriteDebugStatement("File " + fileName + " does not exist");
            return;
        }
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
        }
    }
}
