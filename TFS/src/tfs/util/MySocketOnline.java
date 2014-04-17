/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 *
 * @author laurencewong
 */
public class MySocketOnline extends MySocket {

    private Socket mSocket;
    private DataOutputStream mDataOutput;
    private DataInputStream mDataInput;
    private BufferedReader mDataReader;
    private String mSocketID; // is the ip:port

    public MySocketOnline(String inIp, int inPort) throws UnknownHostException, IOException {
        super(inIp, inPort);
        mSocket = new Socket(inIp, inPort);
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));
        mSocketID = inIp + ":" + inPort;
    }

    public MySocketOnline(Socket inSocket) throws IOException {
        super(inSocket);
        mSocket = inSocket;
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));

        String socketInetAddr = inSocket.getInetAddress().toString();
        mSocketID = socketInetAddr.substring(1, socketInetAddr.length()) + ":" + inSocket.getPort();
    }

    public MySocketOnline(String inInfo) throws UnknownHostException, IOException {
        super(inInfo);
        String[] info = inInfo.split(":");
        mSocket = new Socket(info[0], Integer.valueOf(info[1]));
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));
        mSocketID = inInfo;
    }

    @Override
    public String GetID() {
        return mSocketID;
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

        mDataOutput.writeInt(length);
        if (length > 0) {
            mDataOutput.write(inBytes, offset, length);
        }
    }

    @Override
    public byte[] ReadBytes() throws IOException {
        int byteArrayLength = mDataInput.readInt();
        byte[] readBytes = new byte[byteArrayLength];
        mDataInput.readFully(readBytes);
        return readBytes;
    }

    @Override
    public boolean hasData() throws IOException {
        return mDataReader.ready();
    }

    @Override
    public void close() throws IOException {
        mSocket.close();
    }
}
