/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.util;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Sends a heartbeat to check if server is responsive
 * @author laurencewong
 */
public class HeartbeatSocket extends Thread {

    final int MAX_TIMEOUT = 5;

    private class HeartbeatStatus {

        InetAddress mIP;
        int mPort;
        int mTimeout; // > MAX_TIMEOUT, then declared dead.
        boolean mAlive;
        String mID; //need multiple statuses for same client because their id is not the same
    }

    Timer mTimer;
    DatagramSocket mSocket;
    String mIPAndPort;
    String mID;
    Callbackable mCallback;

    //final ArrayList<HeartbeatStatus> mStatuses;
    final HashMap<String, HeartbeatStatus> mStatuses;

    public HeartbeatSocket(int inPort, Callbackable inCallback) throws IOException {
        mIPAndPort = GetIP() + ":" + inPort;
        System.out.println("Starting heartbeat socket on: " + mIPAndPort);
        //initialize the listener socket
        mSocket = new DatagramSocket(inPort);
        mStatuses = new HashMap<>();
        mTimer = new Timer();
        mID = "servermaster";
        mCallback = inCallback;
    }

    public HeartbeatSocket(String inID, String inTheirID, Callbackable inCallback) throws IOException {
        mSocket = new DatagramSocket(null);
        mSocket.bind(null);
        mIPAndPort = GetIP() + ":" + ((InetSocketAddress) mSocket.getLocalSocketAddress()).getPort();
        System.out.println("Starting heartbeat socket on: " + mIPAndPort);
        System.out.println("With ID: " + inID);
        //initialize the listener socket
        mStatuses = new HashMap<>();
        mTimer = new Timer();
        mID = inID;
        mCallback = inCallback;
        AddHeartbeat(inTheirID, inTheirID);
    }
    /**
     * Gets the IP address
     * @return string containing IP address
     * @throws IOException 
     */
    public final String GetIP() throws IOException {
        String ip = InetAddress.getLocalHost().toString();
        ip = ip.substring(ip.indexOf("/") + 1);
        return ip;
    }
    /**
     * Receives a response message from the server
     * @return Message object containing response
     * @throws IOException 
     */
    public Message ReceiveMessage() throws IOException {
        byte[] receiveData = new byte[128]; // shouldn't be sending that much info
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        mSocket.receive(receivePacket);
        return new Message(receivePacket.getData());
    }
    /**
     * Adds server to list in order to be checked by heartbeat
     * @param inID
     * @param inIPAndPort 
     */
    public final void AddHeartbeat(String inID, String inIPAndPort) {
        try {
            //
            String[] idSplit = inIPAndPort.split(":");
            HeartbeatStatus newStatus = new HeartbeatStatus();
            String nameAndIp = InetAddress.getByName(idSplit[0].trim()).toString();
            nameAndIp = nameAndIp.substring(nameAndIp.indexOf("/") + 1);
            if (nameAndIp.equalsIgnoreCase("localhost")) {
                nameAndIp = "127.0.0.1";
            }
            //System.out.println("Name and IP And Port: " + nameAndIp + ":" + idSplit[1].trim());
            newStatus.mIP = InetAddress.getByName(nameAndIp);
            newStatus.mPort = Integer.valueOf(idSplit[1].trim());
            newStatus.mID = inID;
            newStatus.mAlive = true;
            newStatus.mTimeout = 0;
            synchronized (mStatuses) {
                mStatuses.put(inID, newStatus);
            }
        } catch (UnknownHostException uhe) {
            System.out.println("Heartbeat found unknonw host: " + inID);
            System.out.println(uhe.getMessage());
        }
    }
    
    public String GetIPAndPort() {
        return mIPAndPort;
    }

    public void Stop() throws IOException, InterruptedException {
        mSocket.close();
        mTimer.cancel();
        this.join();
    }
    /**
     * Launches timer which sends out heartbeat messages
     */
    private void StartUpdater() {
        mTimer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                synchronized (mStatuses) {
                    for (HeartbeatStatus hs : mStatuses.values()) {
                        hs.mTimeout++;
                    }
                    CullDeadHeartbeats();
                    SendHeartbeat();
                }
            }
        }, 0, 250);
    }
    /**
     * Sends a heartbeat message to all servers
     */
    private void SendHeartbeat() {
        try {
            Message m = new Message();
            synchronized (mStatuses) {
                for (HeartbeatStatus hs : mStatuses.values()) {
                    m.ResetWriteHead();
                    m.ResetReadHead();
                    m.WriteString(mID);
                    m.WriteString(mIPAndPort);
                    byte[] messageData = m.ReadData();
                    DatagramPacket heartbeat = new DatagramPacket(messageData, messageData.length, hs.mIP, hs.mPort);
                    mSocket.send(heartbeat);
                    //System.out.println("HB: " + mID + " Sent to " + hs.mIP.toString() + ":" + hs.mPort);
                }
            }
        } catch (IOException ioe) {
            System.out.println("Unable to send heartbeat ");
            System.out.println(ioe.getMessage());
        }
    }
    /**
     * Returns true if a new heartbeat is found
     * @param inID
     * @return true or false
     */
    private boolean IsNewHeartbeat(String inID) {
        synchronized (mStatuses) {
            if (mStatuses.containsKey(inID)) {
                return false;
            }
        }
        //System.out.println("Found new heartbeat: " + inID);
        return true;
    }
    /**
     * Resets timeout once heartbeat is received
     * @param inID 
     */
    private void UpdateHeartbeat(String inID) {
        synchronized (mStatuses) {
            mStatuses.get(inID).mTimeout = 0;
        }
    }
    /**
     * Culls any servers that have not responded for set number of seconds (MAX_TIMEOUT)
     */
    private void CullDeadHeartbeats() {
        ArrayList<HeartbeatStatus> deadStatuses = new ArrayList<>();
        synchronized (mStatuses) {
            for (HeartbeatStatus hs : mStatuses.values()) {
                if (hs.mTimeout > MAX_TIMEOUT) {
                    System.out.println(hs.mID + " has not responded for " + MAX_TIMEOUT + " seconds; disconnecting");
                    deadStatuses.add(hs);
                }
            }
            for (HeartbeatStatus hs : deadStatuses) {
                mStatuses.remove(hs.mID);
                mCallback.Callback(hs.mID);
            }
        }
    }

    @Override
    public void run() {
        try {
            StartUpdater();
            while (true) {
                Message received = ReceiveMessage();
                String receivedID = received.ReadString();
                String receivedIPAndPort = received.ReadString();
                //System.out.println("ID:" + receivedID + " ipAndPort: " + receivedIPAndPort);

                if (IsNewHeartbeat(receivedID)) {
                    //System.out.println("ID:" + receivedID + " ipAndPort: " + receivedIPAndPort);
                    AddHeartbeat(receivedID, receivedIPAndPort);
                } else {
                    UpdateHeartbeat(receivedID);
                }
            }
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
    }
}
