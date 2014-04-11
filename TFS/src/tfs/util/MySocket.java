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

    public MySocket(String inIp, int inPort) throws UnknownHostException, IOException {
        mSocket = new Socket(inIp, inPort);
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));
    }

    public MySocket(Socket inSocket) throws IOException {
        mSocket = inSocket;
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));
    }

    public void WriteMessage(Message m) throws IOException {
        //Write any debug statements if they exist
/*        Message newMessage = new Message();
        ArrayList<String> debugStatements = m.GetDebugStatements();
        newMessage.WriteInt(debugStatements.size());
        for (String s : m.GetDebugStatements()) {
            newMessage.WriteString(s);
        }
        newMessage.AppendData(m.ReadData());
        */
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
