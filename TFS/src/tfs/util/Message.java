/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.util;

import java.io.*;
import java.nio.ByteBuffer;
import tfs.util.MySocket;

/**Message class holds a generic byte array
 * Acts like a one way stack that holds generic data
 * Even though it says serializable, shouldn't serialize it for socket use
 * Set the data[] buffer from the socket;
 *
 * @author laurencewong
 */
public class Message implements Serializable {

    /**
     * Serialize an object and return it as a byte buffer
     * @param o The object to serialize
     * @return The object that was given as a byte array
     * @throws IOException 
     */
    public static byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(bs);
        os.writeObject(o);
        return bs.toByteArray();
    }

    /**
     * Does the opposite of serialization and returns a generic object
     * @param b The byte array to deserialize
     * @return The object that was deserialized
     * @throws IOException 
     */
    public static Object deserialize(byte[] b) throws IOException {
        ByteArrayInputStream bs = new ByteArrayInputStream(b);
        ObjectInputStream os = new ObjectInputStream(bs);
        try {
            return os.readObject();
        } catch (ClassNotFoundException cnfe) {
            System.out.println("Unable to deserialize bytes");
            System.out.println(cnfe.getMessage());
        }
        return null;
    }

    private MySocket mSocket = null; //socket to send message on
    private int mReadHead = 0;
    private int mWriteHead = 0;
    private byte[] mByteContents;
   

    public Message() {
       mByteContents = new byte[0];
    }
   
    public Message(byte[] inArray) {
        mByteContents = inArray;
        mWriteHead = inArray.length; //assumes that inArray is full
        
        //Read any debug statements that came in with the message
        /*int numDebugStatements = ReadInt();
        for(int i = 0; i < numDebugStatements; ++i) {
            System.out.println(ReadString());
        }
        */
    }
    
    public void SetSocket(MySocket inSocket) {
        mSocket = inSocket;
    }
    
    public void Send() throws IOException {
        mSocket.WriteMessage(this);
    }

    /**
     * Append data to the internal byte array.  Automatically resizes if the
     * buffer is too small to accommodate the new data
     * @param inData The data to add to the byte array
     */
    public void AppendData(byte[] inData) {
        if(mByteContents.length == 0) {
            mByteContents = new byte[inData.length];
        }
        while (mWriteHead + inData.length > mByteContents.length) {
            //must resize byte array
            byte[] newByteArray = new byte[mByteContents.length * 2];
            System.arraycopy(mByteContents, 0, newByteArray, 0, mWriteHead);
            mByteContents = newByteArray;
        }

        System.arraycopy(inData, 0, mByteContents, mWriteHead, inData.length);
        mWriteHead += inData.length;
    }
    
    /**
     * Reads the entire byte array and returns it.  Will only work if mReadHead
     * is at the beginning of the buffer
     * @return it
     */
    public byte[] ReadData() {
        return ReadData(mWriteHead);
    }
    
    /**
     * Reads a specific amount of data and returns it as a byte array
     * @param length the length of data to read from the array
     * @return the data from the read head to the given length
     * @throws IllegalArgumentException 
     */
    public byte[] ReadData(int length) throws IllegalArgumentException{
        if(length + mReadHead > mWriteHead || length < 0) {
            throw new IllegalArgumentException("cannot read length of " + length);
        }
        
        byte[] returnArray = new byte[length];
        System.arraycopy(mByteContents, mReadHead, returnArray, 0, length);
        
        mReadHead += length;
        return returnArray;
    }

    /**
     * Writes an integer
     * @param i the integer
     */
    public void WriteInt(int i) {
        AppendData(ByteBuffer.allocate(4).putInt(i).array());
    }
    
    public int ReadInt() {
        byte[] intData = ReadData(4);
        return ByteBuffer.wrap(intData).getInt();
    }

    /**
     * Writes a string to the byte array
     * @param inString The string that you're writing
     */
    public void WriteString(String inString) {
        byte[] stringAsByteArray = inString.getBytes();
        AppendData(ByteBuffer.allocate(4).putInt(stringAsByteArray.length).array());
        AppendData(stringAsByteArray);
    }
    
    /**
     * Reads a string from the byte array.  Needs to read the size of the string
     * first
     * @return The string that was read from the array
     */
    public String ReadString() {
        int stringLength = 0;
        stringLength = ByteBuffer.wrap(ReadData(4)).getInt(); //Read in the size of the string
        byte[] stringData = ReadData(stringLength);
        return new String(stringData);
    }

    /**
     * Write a generic object to the byte array
     * @param inObject The generic object
     */
    public void WriteObject(Object inObject) {
        try {
            AppendData(Message.serialize(inObject));
        } catch (IOException ioe) {
            System.out.println("Problem writing object into message");
            System.out.println(ioe.getMessage());
        }
    }
    
    public void WriteDebugStatement(String inDebugStatement) {
        WriteString("Print");
        WriteString(inDebugStatement);
    }
    
    public boolean isEmpty() {
        return (mByteContents.length == 0);
    }
    public boolean isFinished() {
        return (mReadHead == mWriteHead);
    }
    
    public void ResetReadHead() {
        mReadHead = 0;
    }
    public void ResetWriteHead() {
        mWriteHead = 0;
    }
}
