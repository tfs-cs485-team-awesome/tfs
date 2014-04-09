/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package servermaster;

import java.net.*;
import java.io.*;
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

    private class ServerMasterClientThread extends Thread {

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
                            
                            /*String clientSentence;
                            String capitalizedSentence;
                            BufferedReader inFromClient
                                    = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                            DataOutputStream outToClient = new DataOutputStream(clientSocket.getOutputStream());*/
                            
                            
                            if (clientSocket.hasData()) {
                                Message messageReceived = new Message(clientSocket.ReadBytes());
                                Message messageSending = new Message();
                                String clientMessage = messageReceived.ReadString();
                                System.out.println("Received: " + clientMessage);
                                messageSending.WriteString("Server Received: " + clientMessage);
                                if (clientMessage == "exit" || clientMessage == "Exit") {

                                    System.out.println("Closing connection with client " + mClientNum);
                                    clientSocket.close();
                                    continue;

                                }
                                String toClient = ParseClientInput(clientMessage);
                                messageSending.WriteString(toClient);
                                //capitalizedSentence = clientSentence.toUpperCase() + '\n';
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
                    //e.printStackTrace();
                }

            }
        }

        /**
         *
         * @param input should be in the format "CommandName Parameters"
         */
        public String ParseClientInput(String input) {
            String output = "";
            String[] inputTokens = input.split(" ");
            System.out.println("Parsing client input");
            if (inputTokens.length < 2) {
                System.out.println("Not enough command line parameters");
                return "";
            }
            switch (inputTokens[0]) {
                case "CreateNewDirectory":
                case "createnewdirectory":
                case "mkdir":
                    CreateNewDir(inputTokens[1]);
                    output = ""; //need to change this to something to output to client
                    break;
                case "ListFiles":
                case "listfiles":
                case "ls":
                    ListFiles(inputTokens[1]);
                    break;
                case "ReadFile":
                    output = ReadFile(inputTokens[1]);
                    break;
            }
            System.out.println("Finished client input");
            return output;
        }

        public String ReadFile(String name) {

            int firstIndex = name.indexOf("/");
            if (firstIndex != 0) {
                System.out.println("Invalid name");
                return "localhost:6999";
                //return "";
            }
            FileNode fileToRead = GetAtPath(name);
            if (fileToRead != null) {
                //add code to read chunk and stuff

                //temp code to tell client to read from chunk server
                return "localhost:6999";
            }
            return "localhost:6999";

//            return null;
        }

        public void CreateNewDir(String name) {
            // check that the first "/" is in the right place
            int firstIndex = name.indexOf("/");
            if (firstIndex != 0) {
                System.out.println("Invalid name");
                return;
            }

            // check if the given directory already exists
            if (GetAtPath(name) != null) {
                System.out.println("Directory already exists");
                return;
            }

            // check that the last "/" exists
            int lastIndex = name.lastIndexOf("/");
            if (lastIndex < 0) {
                System.out.println("Invalid name");
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
                    return;
                }
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

        public FileNode GetAtPath(String filePath) {
            if (filePath.indexOf("/") != 0) {
                return null;
            }
            String[] filePathTokens = filePath.split("/");
            FileNode curFile = mMaster.mFileRoot;
            for (int i = 1; i < filePathTokens.length; ++i) {
                String dir = filePathTokens[i];
                boolean dirExists = false;
                for (FileNode file : curFile.mChildren) {
                    if (file.mName.equalsIgnoreCase(dir)) {
                        curFile = file;
                        dirExists = true;
                        break;
                    }
                }
                if (!dirExists) {
                    //System.out.println("Invalid path");
                    return null;
                }
            }
            return curFile;
        }

        public void ListFiles(String filePath) {
            System.out.println("Listing files for path: " + filePath);
            FileNode fileDir = GetAtPath(filePath);
            if (fileDir != null) {
                for (FileNode file : fileDir.mChildren) {
                    System.out.println(file.mName);
                }
            }
        }
    }
}
