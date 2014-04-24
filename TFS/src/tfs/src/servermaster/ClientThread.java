/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.servermaster;

import java.io.*;
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

    MySocket mSocket;
    ArrayDeque<Message> mPendingMessages;
    FileNode mFileRoot;
    ServerMaster mMaster;

    public final void Init() {
        mPendingMessages = new ArrayDeque<>();
    }

    public ClientThread(MySocket inSocket, ServerMaster inMaster, FileNode inNode) {
        mSocket = inSocket;
        mFileRoot = inNode;
        mMaster = inMaster;
        Init();
    }

    public void Close() throws IOException {
        mSocket.close();
    }

    @Override
    public void run() {
        try {
            if (mSocket.hasData()) {
                Message messageReceived = new Message(mSocket.ReadBytes());
                Message messageSending = ParseClientInput(messageReceived);
                messageSending.SetSocket(mSocket);
                mPendingMessages.push(messageSending);
            }
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
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

    public void RecursiveDeleteDirectory(String path, FileNode file, ArrayList<String> allDirs, ArrayList<String> allFiles, Message m) {
        // delete all children in the directory
        for (FileNode child : file.mChildren) {
            if (child.mIsDirectory) {
                allDirs.add(path + "/" + child.mName);
                RecursiveDeleteDirectory(path + "/" + child.mName, child, allDirs, allFiles, m);
            } else {
                allFiles.add(path + "/" + child.mName);
            }
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

    public void AssignChunkServerToFile(String fileName) {
        //make a random chunk server the location of this new file
        /*
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
         */

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
                String name = m.ReadString();
                int lengthToRead = m.ReadInt();
                byte[] data = m.ReadData(lengthToRead);
                WriteFile(name, data, outputToClient);
                break;
            }
            case "stream":
                AppendFile(m.ReadString(), outputToClient);
                break;
            case "getnode": {
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
    }

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
    }

    public void AppendFile(String fileName, Message output) {
        FileNode file = GetAtPath(fileName);
        if (file == null) {
            CreateNewFile(fileName, output);
        }
        //TODO error check if the file.GetChunkDataAtIndex return null
        FileNode.ChunkMetadata chunk = file.GetChunkDataAtIndex(0);
        if (chunk == null) {
            output.WriteDebugStatement(("Chunk not found"));
            return;
        }
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
    }

    public void WriteFile(String fileName, byte[] data, Message output) {

        FileNode file = GetAtPath(fileName);
        if (file != null) {
            System.out.println("File already exists: " + fileName);
            output.WriteDebugStatement("File already exists: " + fileName);
            return;
        }
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
        }
    }

    public void ReadFile(String fileName, Message output) {
        System.out.println("Sending back info for readfile");
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
        // check if the path is a file
        if (!file.mIsDirectory) {
            DeleteFile(path, m);
            return;
        }
        ArrayList<String> allFiles = new ArrayList<>();
        ArrayList<String> allDirs = new ArrayList<>();
        allDirs.add(path);
        // find all children in the directory
        for (FileNode child : file.mChildren) {
            if (child.mIsDirectory) {
                allDirs.add(path + "/" + child.mName);
                RecursiveDeleteDirectory(path + "/" + child.mName, child, allDirs, allFiles, m);
            } else {
                allFiles.add(path + "/" + child.mName);
            }
        }
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
}
