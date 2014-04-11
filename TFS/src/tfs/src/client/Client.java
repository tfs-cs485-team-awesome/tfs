/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.client;

import java.net.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import tfs.util.Message;
import tfs.util.MySocket;
import tfs.util.FileNode;

/**
 *
 * @author laurencewong
 */
public class Client implements ClientInterface {

    Stack<String> mCurrentPath;
    String mServerIp;
    String sentence = "";
    int mServerPortNum;
    MySocket serverSocket;

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

        mServerIp = parts[0];
        mServerPortNum = Integer.valueOf(parts[1]);
        mCurrentPath = new Stack<>();
        mCurrentPath.add("");
    }

    public void InitConnectionWithServer(MySocket inSocket) {
        try {
            System.out.println("Telling server that I am a client");
            Message toServer = new Message();
            toServer.WriteString("Client");
            inSocket.WriteMessage(toServer);
        } catch (IOException ioExcept) {
            System.out.println("Problem initializing connection with server");
            System.out.println(ioExcept.getMessage());
        }
    }

    /**
     *
     */
    public boolean ReceiveMessage() throws IOException {
        if (serverSocket.hasData()) {
            Message fromServer = new Message(serverSocket.ReadBytes());
            while (!fromServer.isFinished()) {
                //message has data
                ParseServerInput(fromServer);
            }
            return true;
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

        sentence = "";
        return false;
    }

    public void ConnectToServer() throws IOException {
        serverSocket = new MySocket(mServerIp, mServerPortNum);
        InitConnectionWithServer(serverSocket);
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
                    SendMessage();
                    ReceiveMessage();
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
        switch (command) {
            case "CreateNewDirectory":
            case "createnewdirectory":
            case "mkdir":
                return ParseCreateNewDir(inStrings, toServer);
            case "CreateNewFile":
            case "createnewfile":
            case "touch":
                return ParseCreateNewFile(inStrings, toServer);
            case "DeleteFile":
            case "deletefile":
            case "rm":
                return ParseDeleteFile(inStrings, toServer);
            case "ListFiles":
            case "listfiles":
            case "ls":
                return ParseListFiles(inStrings, toServer);
            case "SeekFile":
            case "seekfile":
            case "seek":
                return ParseSeekFile(inStrings, toServer);
            case "ReadFile":
            case "readfile":
            case "read":
                return ParseReadFile(inStrings, toServer);
            case "Append":
            case "append":
            case "appendtofile":
            case "AppendToFile":
                return ParseAppendFileToFile(inStrings, toServer);
            case "WriteFile":
            case "writetofile":
            case "write":
                return ParseWriteToFile(inStrings, toServer);
            case "GetNode":
                return ParseGetNode(inStrings, toServer);
            case "GetFilesUnderPath":
                return ParseFilesUnderNode(inStrings, toServer);
            case "cd":
            case "dir":
                return ParseTestPath(inStrings, toServer);
            case "pwd":
                System.out.println(GetCurrentPath());
                return false;
            case "LogicalFileCount":
                return ParseLogicalFileCount(inStrings, toServer);
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
        toServer.WriteString(inString[2]);

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

    public void ParseServerInput(Message m) throws IOException {
        String input = m.ReadString();
        switch (input) {
            case "Print":
                //print out a debug statement
                System.out.println(m.ReadString());
                break;
            case "ChunkInfo":
                ContactChunkServer(m);
                break;
            case "WriteFileResponse":
                WriteFileResponse(m);
                break;
            case "SeekFileResponse":
                //name datalen data
                SeekFileResponse(m);
                break;
            case "GetNodeResponse": {
                try {
                    mTempFileNode = new FileNode(false);
                    mTempFileNode.ReadFromMessage(m);

                } catch (IOException ioe) {
                    System.out.println("Problem deserializing file node");
                    System.out.println(ioe.getMessage());
                }
                break;
            }
            case "GetFilesUnderPathResponse": {
                mTempFilesUnderNode = GetFilesUnderNodeResponse(m);
                break;
            }
            case "ReadFileResponse":
                ReadFileResponse(m);
                break;
            case "TestPathResponse":
                TestPathResponse(m);
                break;
            case "LogicalFileCountResponse":
                LogicalFileCountResponse(m);
                break;
        }
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

    public void ContactChunkServer(Message m) {
        MySocket chunkServerSocket = null;
        String chunkServerInfo = m.ReadString();
        System.out.println("Contacting chunk server with param " + chunkServerInfo);
        if (chunkServerInfo.contentEquals("localhost:6999")) {
            String[] splitInput = chunkServerInfo.split(":");
            try {
                Message toChunkServer = new Message();
                System.out.println("Connecting to chunkServer at: " + chunkServerInfo);
                chunkServerSocket = new MySocket(splitInput[0], Integer.valueOf(splitInput[1]));

                InitConnectionWithServer(chunkServerSocket);

                toChunkServer.WriteString("ReadFile");
                chunkServerSocket.WriteMessage((toChunkServer));

                chunkServerSocket.close();
            } catch (Exception e) {
                System.out.println("Problem writing byte");
                System.out.println(e.getMessage());
            }
        }
    }

    public void ReadFileResponse(Message m) throws IOException {
        String filename = m.ReadString();
        int bytesToRead = m.ReadInt();
        byte[] bytesRead = m.ReadData(bytesToRead);
        WriteLocalFile(filename, bytesRead);
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

}