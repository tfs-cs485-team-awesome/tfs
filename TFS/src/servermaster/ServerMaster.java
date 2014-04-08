/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package servermaster;

import java.net.*;
import java.io.*;
import java.util.ArrayList;

/**
 *
 * @author laurencewong
 */
public class ServerMaster {

    FileNode mFileRoot;
    ServerSocket mListenSocket;
    ArrayList<Socket> mClients;

    public ServerMaster(int inSocketNum) {
        mFileRoot = new FileNode(false);
        mFileRoot.mName = "";
        mClients = new ArrayList<Socket>();
        try {
            System.out.println("Starting server on port " + inSocketNum);
            mListenSocket = new ServerSocket(inSocketNum);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    public ServerMaster() {
        mFileRoot = new FileNode(false);
        mFileRoot.mName = "/";
        mClients = new ArrayList<Socket>();
        try {
            mListenSocket = new ServerSocket(6789);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    public void RunLoop() {
        String clientSentence;
        String capitalizedSentence;

        System.out.println("Starting server");
        try {
            ServerMasterClientThread newClientThread = new ServerMasterClientThread(this);
            newClientThread.start();
            while (true) {
                Socket newClient = mListenSocket.accept();
                synchronized (mClients) {
                    mClients.add(newClient);
                    System.out.println("Adding new client");
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
                        for (Socket clientSocket : mMaster.mClients) {
                            String clientSentence;
                            String capitalizedSentence;
                            BufferedReader inFromClient
                                    = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                            DataOutputStream outToClient = new DataOutputStream(clientSocket.getOutputStream());
                            clientSentence = inFromClient.readLine();
                            System.out.println("Received: " + clientSentence);
                            if (clientSentence == "exit" || clientSentence == "Exit") {

                                System.out.println("Closing connection with client " + mClientNum);
                                clientSocket.close();
                                continue;

                            }
                            ParseClientInput(clientSentence);
                            capitalizedSentence = clientSentence.toUpperCase() + '\n';
                            outToClient.writeBytes(capitalizedSentence);
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
        public void ParseClientInput(String input) {
            String[] inputTokens = input.split(" ");
            System.out.println("Parsing client input");
            if (inputTokens.length < 2) {
                System.out.println("Not enough command line parameters");
                return;
            }
            switch (inputTokens[0]) {
                case "CreateNewDirectory":
                case "createnewdirectory":
                case "mkdir":
                    CreateNewDir(inputTokens[1], mMaster.mFileRoot);
                    break;
                case "ListFiles":
                case "listfiles":
                case "ls":
                    ListFiles(inputTokens[1]);
                    break;
            }
            System.out.println("Finished client input");
            return;
        }

        public void CreateNewDir(String name, FileNode parentNode) {
            if(GetAtPath(name) != null){
                System.out.println("Directory already exists");
                return;
            }
            
            int index = name.lastIndexOf('/') - 1;
            String parent = name.substring(0, index);
            FileNode file = GetAtPath(parent);
            if(file == null){
                System.out.println("Parent directory does not exist");
                return;
            }
            
            System.out.println("Creating new dir " + name);
            FileNode newDir = new FileNode(false);
            newDir.mIsDirectory = true;
            newDir.mName = name;
            parentNode.mChildren.add(newDir);
            System.out.println("Finished creating new dir");
            return;
        }

// Duplicate functionality compared to GetAtPath
//        public boolean ExistsAtPath(String filePath) {
//            String[] filePathTokens = filePath.split("/");
//            FileNode curDir = mMaster.mFileRoot;
//            for (int i = 0; i < filePathTokens.length; ++i) {
//                String dir = filePathTokens[i];
//                boolean dirExists = false;
//                for (FileNode file : curDir.mChildren) {
//                    if (file.mName == dir) {
//                        curDir = file;
//                        dirExists = true;
//                        break;
//                    }
//                }
//                if (!dirExists) {
//                    System.out.println("Invalid path");
//                    return false;
//                }
//            }
//            return true;
//        }

        public FileNode GetAtPath(String filePath) {
            String[] filePathTokens = filePath.split("/");
            FileNode curFile = mMaster.mFileRoot;
            for (int i = 0; i < filePathTokens.length; ++i) {
                String dir = filePathTokens[i];
                boolean dirExists = false;
                for (FileNode file : curFile.mChildren) {
                    if (file.mName == dir) {
                        curFile = file;
                        dirExists = true;
                        break;
                    }
                }
                if (!dirExists) {
                    System.out.println("Invalid path");
                    return null;
                }
            }
            return curFile;
        }

        public void ListFiles(String filePath) {
            System.out.println("Listing files for path: " + filePath);
            String[] filePathTokens = filePath.split("/");
            FileNode curDir = mMaster.mFileRoot;
            for (int i = 0; i < filePathTokens.length; ++i) {
                String dir = filePathTokens[i];
                boolean dirExists = false;
                for (FileNode file : curDir.mChildren) {
                    if (file.mName == dir) {
                        curDir = file;
                        dirExists = true;
                        break;
                    }
                }
                if (!dirExists) {
                    System.out.println("Invalid path");
                    //hacky
                    return;
                }
            }

            for (FileNode file : curDir.mChildren) {
                System.out.println(file.mName);
            }
        }
    }
}
