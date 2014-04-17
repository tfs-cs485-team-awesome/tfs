/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.util;

import java.io.*;
import java.net.*;

/**
 *
 * @author laurencewong
 */
public class MySocketOffline extends MySocket {

    Message mSocketData; //Place to put data for the socket to read
    Message mDebugData; //MySocketOffline writes to this instead of an actual socket

    public MySocketOffline(String inIp, int inPort) throws UnknownHostException, IOException {
        super(inIp, inPort);
        mSocketData = new Message();
        mDebugData = new Message();
    }

    public MySocketOffline(Socket inSocket) throws IOException {
        super(inSocket);
        mSocketData = new Message();
        mDebugData = new Message();
    }

    @Override
    public String GetID() {
        return "127.0.0.1:6789"; //need to change this
    }

    @Override
    public void WriteMessage(Message m) throws IOException {
        //Write internal data
        byte[] messageBytes = m.ReadData();
        WriteBytes(messageBytes);
    }

    @Override
    public void WriteBytes(byte[] inBytes) throws IOException {
        WriteBytes(inBytes, 0, inBytes.length);
    }

    @Override
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

        mDebugData.WriteInt(length);
        if (length > 0) {
            mDebugData.AppendData(inBytes);
        }
    }

    @Override
    public byte[] ReadBytes() throws IOException {
        int byteArrayLength = mSocketData.ReadInt();
        byte[] readBytes = mSocketData.ReadData(byteArrayLength);
        
        //pretend to empty the socket
        mSocketData.ResetReadHead();
        mSocketData.ResetWriteHead();
        return readBytes;
    }

    @Override
    public boolean hasData() throws IOException {
        return mSocketData.isFinished();
    }

    @Override
    public void close() throws IOException {
        System.out.println("Closing socket");
    }
    
    //Debug Methods --
    
    public void DebugWriteMessage(Message m) {
        byte[] inputData = m.ReadData(); //reads all the data from m
        mSocketData.WriteInt(inputData.length);
        mSocketData.AppendData(inputData);
    }
    
    public byte[] DebugReadMessage() {
        int byteArrayLength = mDebugData.ReadInt();
        byte[] readBytes = mDebugData.ReadData(byteArrayLength);
        return readBytes;
    }
    
    public void DebugResetSocket() {
        mDebugData.ResetReadHead();
        mDebugData.ResetWriteHead();
    }
    
    //-- Debug Methods
}
