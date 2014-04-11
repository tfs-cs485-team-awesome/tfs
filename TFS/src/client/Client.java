/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package client;

import java.net.*;
import java.io.*;
import java.util.ArrayList;
import tfs.Message;
import tfs.MySocket;

/**
 *
 * @author laurencewong
 */
public class Client implements ClientInterface {

    String mServerIp;
    String sentence = "";
    int mServerPortNum;
    MySocket serverSocket;

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
                ParseInput(fromServer);
            }
            return true;
        }
        return false;
    }

    public String[] DebugReceiveMessage() throws IOException {
        ArrayList<String> debugStatements = new ArrayList<String>();

        if (serverSocket.hasData()) {
            Message fromServer = new Message(serverSocket.ReadBytes());

            while (!fromServer.isFinished()) {
                //message has data
                //ParseInput(fromServer);
                debugStatements.add(fromServer.ReadString());
            }
        }
        return (String[]) debugStatements.toArray();

    }

    public void SendMessage() throws IOException {
        Message toServer = new Message();

        if (!sentence.isEmpty()) {
            String[] sentenceTokenized = sentence.split(" ");
            for (String s : sentenceTokenized) {
                toServer.WriteString(s);
            }
            if (ValidMessage(toServer)) {
                toServer.ResetReadHead();
                serverSocket.WriteMessage(toServer);
            }
        }

        sentence = "";
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
                serverSocket = new MySocket(mServerIp, mServerPortNum);

                InitConnectionWithServer(serverSocket);

                while (true) {

                    if (inFromUser.ready()) {
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

    public Boolean ValidMessage(Message m) {
        String input = m.ReadString();
        switch (input) {
            case "CreateNewDirectory":
            case "createnewdirectory":
            case "mkdir":
                return ValidMessageString(m);
            case "CreateNewFile":
            case "createnewfile":
            case "touch":
                return ValidMessageString(m);
            case "DeleteFile":
            case "deletefile":
            case "rm":
                return ValidMessageString(m);
            case "ListFiles":
            case "listfiles":
            case "ls":
                return ValidMessageString(m);
            case "ReadFile":
            case "readfile":
            case "read":
                return ValidMessageIntInt(m);
            case "WriteFile":
            case "writetofile":
            case "write":
                return ValidMessageData(m);
        }
        System.out.println("Not a recognized command");
        return false;
    }

    public Boolean ValidMessageString(Message m) {
        try {
            String parameter = m.ReadString();
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid command");
            return false;
        }
        return true;
    }

    public Boolean ValidMessageInt(Message m) {
        try {
            int parameter = m.ReadInt();
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid command");
            return false;
        }
        return true;
    }

    public Boolean ValidMessageIntInt(Message m) {
        try {
            int parameter1 = m.ReadInt();
            int parameter2 = m.ReadInt();
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid command");
            return false;
        }
        return true;
    }

    public Boolean ValidMessageData(Message m) {
        try {
            String name = m.ReadString();
            int length = m.ReadInt();
            byte[] data = m.ReadData(length);
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid command");
            return false;
        }
        return true;
    }

    public void ParseInput(Message m) {
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
            case "ReadFileResponse":
                ReadFileResponse(m);
                break;
        }
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

    public void WriteFileResponse(Message m) {
        // if master returns chunk to be created, contact chunk server to create chunks

        // after getting chunks, append to end of chunk
    }

    public void ReadFileResponse(Message m) {

    }

    @Override
    public void CreateFile(String fileName) throws IOException {
        sentence = "touch " + fileName;
        SendMessage();
        while(!ReceiveMessage());

    }

    @Override
    public void CreateDir(String dirName) throws IOException {
        sentence = "mkdir " + dirName;
        SendMessage();
        while(!ReceiveMessage());
    }

    @Override
    public void DeleteFile(String fileName) throws IOException {
        sentence = "rm " + fileName;
        SendMessage();
        while(!ReceiveMessage());
    }

    @Override
    public void ListFile(String path) throws IOException {
        sentence = "ls " + path;
        SendMessage();
        while(!ReceiveMessage());
    }

    @Override
    public String[] GetListFile(String path) throws IOException {
        sentence = "ls " + path;
        SendMessage();
        return DebugReceiveMessage();
    }

    @Override
    public void ReadFile(String fileName) throws IOException {
        System.out.println("Not supported yet");
    }

    @Override
    public void WriteFile(String fileName) throws IOException {
        System.out.println("Not supported yet");
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
