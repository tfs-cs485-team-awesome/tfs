/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.util;

import java.net.*;
import java.io.*;
import tfs.util.Message;
import java.util.ArrayList;

/**
 *
 * @author laurencewong
 */
public class MySocket {

    private Socket mSocket;
    private DataOutputStream mDataOutput;
    private DataInputStream mDataInput;
    private BufferedReader mDataReader;
    private String mSocketID; // is the ip:port

   public MySocket(String inIp, int inPort) throws UnknownHostException, IOException {
        
        mSocket = new Socket(inIp, inPort);
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));
        mSocketID = inIp + ":" + inPort;
       
    }
   
    public MySocket(String inInfo) throws UnknownHostException, IOException {
        String[] info = inInfo.split(":");
        mSocket = new Socket(info[0], Integer.valueOf(info[1]));
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));
        mSocketID = inInfo;
    }

    public MySocket(Socket inSocket) throws IOException {
        
        mSocket = inSocket;
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));
        
        String socketInetAddr = inSocket.getInetAddress().toString();
        mSocketID = socketInetAddr.substring(1, socketInetAddr.length()) + ":" + inSocket.getPort();
                
    }
    
    public void SetID(String inID) {
        mSocketID = inID;
    }
    
    public String GetID() {
        
        return mSocketID;
    }

    public void WriteMessage(Message m) throws IOException {
        
        //Write internal data
        byte[] messageBytes = m.ReadData();
        WriteBytes(messageBytes);
    }

    public void WriteBytes(byte[] inBytes) throws IOException {
        
        WriteBytes(inBytes, 0, inBytes.length);
    }

    public void WriteBytes(byte[] inBytes, int offset, int length) throws IllegalArgumentException, IOException {
        if (length == 0) {

            //don't write
            return;
        }

        if (offset < 0 || offset >= length) {
            throw new IllegalArgumentException("Offset cannot be less than 0 or greater than length.  Offset: " + offset);
        }
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be less than 0.  Length: " + length);
        }

        mDataOutput.writeInt(length);
        if (length > 0) {
            mDataOutput.write(inBytes, offset, length);
        }
    }

    public byte[] ReadBytes() throws IOException {
        int byteArrayLength = mDataInput.readInt();
        byte[] readBytes = new byte[byteArrayLength];
        mDataInput.readFully(readBytes);
        return readBytes;
    }

    public boolean hasData() throws IOException {
        return mDataReader.ready();
    }

    public void close() throws IOException {
        mSocket.close();
    }
}
