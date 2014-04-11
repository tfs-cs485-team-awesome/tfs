/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.util;

import java.util.ArrayList;
import java.io.*;

/**
 *
 * @author laurencewong
 */
public class FileNode implements Serializable {

    public FileNode(boolean isFile) {
        mIsDirectory = !isFile;
        if (!isFile) {
            mChildren = new ArrayList<>();
        }
        if (isFile) {
            mFileMetadata = new FileMetadata();
        }
    }

    /**
     * Metadata for each chunk Holds the location of that chunk and the
     * locations of each of the replicas for that chunk
     */
    private class ChunkMetadata {

        String mLocation;
        ArrayList<String> mReplicaLocations;

        public ChunkMetadata() {
            mReplicaLocations = new ArrayList<String>();
        }
    }

    /**
     * Metadata for each file Holds the list of chunks that make up a file
     */
    private class FileMetadata {

        ArrayList<ChunkMetadata> mChunks;

        public FileMetadata() {
            mChunks = new ArrayList<ChunkMetadata>();
        }
    }

    public void WriteToMessage(Message m) throws IOException {
        m.WriteString(mName);
        if (mIsDirectory) {
            m.WriteInt(1);
        } else {
            m.WriteInt(0);
        }
        if (mReadLock) {
            m.WriteInt(1);
        } else {
            m.WriteInt(0);
        }
        if (mWriteLock) {
            m.WriteInt(1);
        } else {
            m.WriteInt(0);
        }

    }

    public void ReadFromMessage(Message m) throws IOException {
        mName = m.ReadString();
        int bool = 0;
        mIsDirectory = false;
        mReadLock = false;
        mWriteLock = false;
        
        bool = m.ReadInt();
        if(bool == 1) {
            mIsDirectory = true;
        }
        bool = m.ReadInt();
        if(bool == 1) {
            mReadLock = true;
        }
        bool = m.ReadInt();
        if(bool == 1) {
            mWriteLock = true;
        }
    }

    public String mName;
    public boolean mIsDirectory;

    //false = unlocked
    //true  = locked
    public boolean mReadLock, mWriteLock;
    public ArrayList<FileNode> mChildren;

    //only need to make this if this is actually a file
    public FileMetadata mFileMetadata;

}
