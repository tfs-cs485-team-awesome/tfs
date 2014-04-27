/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.util;

import java.net.*;
import java.io.*;
/**
 * MySocket class represents MySocket object
 * @author laurencewong
 */
public class MySocket implements Callbackable{

    private Socket mSocket;
    private DataOutputStream mDataOutput;
    private DataInputStream mDataInput;
    private BufferedReader mDataReader;
    private String mSocketID; // is the ip:port

    /**
     * MySocket Constructor
     * @param inIp socket IP address
     * @param inPort socket Port number
     * @throws UnknownHostException
     * @throws IOException 
     */
   public MySocket(String inIp, int inPort) throws UnknownHostException, IOException {
        
        mSocket = new Socket(inIp, inPort);
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));
        mSocketID = inIp + ":" + inPort;
       
    }
   
   /**
    * MySocket Constructor
    * @param inInfo string containing IP address and Port number of socket
    * @throws UnknownHostException
    * @throws IOException 
    */
    public MySocket(String inInfo) throws UnknownHostException, IOException {
        String[] info = inInfo.split(":");
        mSocket = new Socket(info[0], Integer.valueOf(info[1]));
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));
        mSocketID = inInfo;
    }

    /**
     * MySocket Constructor
     * @param inSocket the Socket
     * @throws IOException 
     */
    public MySocket(Socket inSocket) throws IOException {
        
        mSocket = inSocket;
        mDataOutput = new DataOutputStream(mSocket.getOutputStream());
        mDataInput = new DataInputStream(mSocket.getInputStream());
        mDataReader = new BufferedReader(new InputStreamReader(mDataInput));
        String socketInetAddr = inSocket.getInetAddress().toString();
        mSocketID = socketInetAddr.substring(1, socketInetAddr.length()) + ":" + inSocket.getPort();
                
    }
    
    /**
     * Setter function for mSocketID
     * @param inID the ID mSocketID is set to
     */
    public void SetID(String inID) {
        mSocketID = inID;
    }
    
    /**
     * Getter function for mSocketID
     * @return mSocketID
     */
    public String GetID() {
        
        return mSocketID;
    }

    /**
     * Writes a message to a byte array
     * @param m the message
     * @throws IOException 
     */
    public void WriteMessage(Message m) throws IOException {
        
        //Write internal data
        byte[] messageBytes = m.ReadData();
        WriteBytes(messageBytes);
    }

    /**
     * Writes byte array to output by calling WriteBytes
     * @param inBytes the byte array
     * @throws IOException 
     */
    public void WriteBytes(byte[] inBytes) throws IOException {
        
        WriteBytes(inBytes, 0, inBytes.length);
    }

    /**
     * Write byte array to output
     * @param inBytes the byte array
     * @param offset the offset of the array
     * @param length the length of the array
     * @throws IllegalArgumentException
     * @throws IOException 
     */
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

    /**
     * Reads byte array
     * @return readBytes the byte array
     * @throws IOException 
     */
    public byte[] ReadBytes() throws IOException {
        int byteArrayLength = mDataInput.readInt();
        byte[] readBytes = new byte[byteArrayLength];
        mDataInput.read(readBytes);
        return readBytes;
    }

    /**
     * Checks if mDataReader has data to read
     * @return true if mDataReader is ready
     * @throws IOException 
     */
    public boolean hasData() throws IOException {
        return mDataReader.ready();
    }

    /**
     * Closes socket
     * @throws IOException 
     */
    public void close() throws IOException {
        mSocket.close();
    }
    
    /**
     * Getter function for mSocket
     * @return mSocket
     */
    public Socket GetSocket() {
        return mSocket;
    }

    @Override
    public void Callback(String inParameter) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
