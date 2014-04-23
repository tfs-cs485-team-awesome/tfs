/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tfs.util;

import java.io.*;
import java.util.ArrayList;

/**
 *
 * @author laurencewong
 */
public class ChunkPendingAppend {
    
    private class ServerStatus {
        boolean hasReplied = false;
        boolean isSuccessful = false;
        MySocket serverSocket;
        
        public ServerStatus(MySocket inServerSocket) {
            serverSocket = inServerSocket;
        }
        
        public void SetSuccessful(boolean inSuccess) {
            isSuccessful = inSuccess;
            hasReplied = true;
        }
        
        public boolean GetSuccessful() {
            return isSuccessful;
        }
        
        public boolean GetReplied() {
            return hasReplied;
        }
        
        public String GetID() {
            return serverSocket.GetID();
        }
        
        public MySocket GetSocket() {
            return serverSocket;
        }
    }
    
    
    ArrayList<ServerStatus> mStatuses;
    String mFilename;
    Message mAppendMessage;
    
    public ChunkPendingAppend(MySocket[] inServerIDs, String inFilename) {
        mFilename = inFilename;
        mStatuses = new ArrayList<>();
        mAppendMessage = new Message();
        for(MySocket s : inServerIDs) {
            mStatuses.add(new ServerStatus(s));
        }
    }
    
    public ChunkPendingAppend(String inFilename) {
        mFilename = inFilename;
        mStatuses = new ArrayList<>();
        mAppendMessage = new Message();
    }
    
    public void AddServer(MySocket inServerSocket) {
        mStatuses.add(new ServerStatus(inServerSocket));
    }
    
    public void UpdateServerStatus(String inServerID, boolean isSucccessful, String inFilename) {
        if(!inFilename.equalsIgnoreCase(mFilename)) {
            System.out.println("Attempted to update a server's status in a pendingappend not for that file");
            return;
        }
        for(ServerStatus ss : mStatuses) {
            if(ss.GetID().equalsIgnoreCase(inServerID)) {
                if(ss.GetReplied() == true) {
                    System.out.println("Server already replied.  something went wrong");
                }
                ss.SetSuccessful(isSucccessful);
                break;
            }
        }
    }
    
    public boolean AllServersReplied() {
        for(ServerStatus ss : mStatuses) {
            if(ss.GetReplied() == false) {
                return false;
            }
        }
        return true;
    }
    
    public String GetFilename() {
        return mFilename;
    }
    
    public boolean WasSuccessful() {
        for(ServerStatus ss : mStatuses) {
            if(ss.GetSuccessful() == false) {
                return false;
            }
        }
        return true;
    }
    
    public Message GetMessage() {
        return mAppendMessage;
    }
    
    public void SendRequest() throws IOException {
        for(ServerStatus ss : mStatuses) {
            mAppendMessage.ResetReadHead();
            ss.GetSocket().WriteMessage(mAppendMessage);
        }
    }
}
