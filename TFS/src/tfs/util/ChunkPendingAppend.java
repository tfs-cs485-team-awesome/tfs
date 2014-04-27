/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tfs.util;

import java.io.*;
import java.util.ArrayList;

/**
 * Keeps track of which servers have replied with successful appends.
 * @author laurencewong
 */
public class ChunkPendingAppend {
    /**
     * Object to keep track of server's status.
     */
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
    long mTimestamp;

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
    /**
     * Adds a server to the list.
     * @param inServerSocket 
     */
    public void AddServer(MySocket inServerSocket) {
        mStatuses.add(new ServerStatus(inServerSocket));
    }
    /**
     * Updates status of server depending on success of append.
     * @param inServerID Server's ID
     * @param isSucccessful Whether append was successful
     * @param inFilename
     */
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
    /**
     * Updates the time stamp.
     * @param inTimestamp 
     */
    public void UpdateTimeStamp(long inTimestamp) {
        mTimestamp = inTimestamp > mTimestamp ? inTimestamp : mTimestamp;
    }
    /**
     * Checks if all servers have replied.
     * @return returns true or false
     */
    public boolean AllServersReplied() {
        for(ServerStatus ss : mStatuses) {
            if(ss.GetReplied() == false) {
                return false;
            }
        }
        return true;
    }
    /**
     * Gets the file name
     * @return string containing file name
     */
    public String GetFilename() {
        return mFilename;
    }
    /**
     * Returns whether append was successful based on server status
     * @return true or false depending on success
     */
    public boolean WasSuccessful() {
        for(ServerStatus ss : mStatuses) {
            if(ss.GetSuccessful() == false) {
                return false;
            }
        }
        return true;
    }
    /**
     * Gets message for appending
     * @return Message
     */
    public Message GetMessage() {
        return mAppendMessage;
    }
    /**
     * Sends a request by writing a message to append
     * @throws IOException 
     */
    public void SendRequest() throws IOException {
        for(ServerStatus ss : mStatuses) {
            mAppendMessage.ResetReadHead();
            ss.GetSocket().WriteMessage(mAppendMessage);
        }
    }
}
