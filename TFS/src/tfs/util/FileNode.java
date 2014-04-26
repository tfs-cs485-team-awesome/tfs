/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.util;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.locks.*;

/**
 *
 * @author laurencewong
 */
public class FileNode{

    public FileNode(boolean isFile) {
        mIsDirectory = !isFile;
        if (!isFile) {
            mChildren = new ArrayList<>();
        }
        if (isFile) {
            mFileMetadata = new FileMetadata();
        }
        mLock = new ReentrantReadWriteLock();
    }

    public class ChunkMetadata {

        ArrayList<String> mChunkReplicaLocations;
        String mChunkLocation;
        long mTimestamp = 0;

        public ChunkMetadata(String inLocation) {
            mChunkReplicaLocations = new ArrayList<>();
            mChunkLocation = inLocation;
        }

        public String GetPrimaryLocation() {
            return mChunkLocation;
        }

        public ArrayList<String> GetReplicaLocations() {
            return mChunkReplicaLocations;
        }
        
        public void UpdateTimestamp(long inTimestamp) {
            if(mTimestamp > inTimestamp) {
                System.out.println("Trying to update chunk with older timestamp");
            }
            mTimestamp = inTimestamp;
        }
        
        public long GetTimestamp() {
            return mTimestamp;
        }

        public void RemoveChunkLocation(String inLocation) {
            if (inLocation.equalsIgnoreCase(mChunkLocation)) {
                System.out.println("Removing primary location");
                mChunkLocation = null;
                if (!mChunkReplicaLocations.isEmpty()) {
                    System.out.println("Replica is becoming primary");
                    mChunkLocation = mChunkReplicaLocations.get(0);
                    RemoveReplicaLocation(mChunkLocation);
                }
            } else {
                RemoveReplicaLocation(inLocation);
            }
        }

        public void RemoveReplicaLocation(String inLocation) {
            String foundLocation = null;
            for (String s : mChunkReplicaLocations) {
                if (s.equalsIgnoreCase(inLocation)) {
                    System.out.println("Removing replica location");
                    foundLocation = s;
                    break;
                }
            }
            if (foundLocation != null) {
                mChunkReplicaLocations.remove(foundLocation);
            }
        }
        
        public void SetChunkLocation(String inLocation) {
            if(mChunkLocation != null) {
                //it has a primary location
                AddReplicaLocation(inLocation);
                return;
            }
            System.out.println("Setting chunk's main location");
            mChunkLocation = inLocation;
        }
        
        public void AddReplicaLocation(String inLocation) {
            for (String s : mChunkReplicaLocations) {
                if (s.equalsIgnoreCase(inLocation)) {
                    System.out.println("Replica already exists");
                    return;
                }
            }
            System.out.println("Adding replica to chunk");
            mChunkReplicaLocations.add(inLocation);
        }
    }

    /**
     * Metadata for each file Holds the list of chunks that make up a file
     */
    public class FileMetadata {

        ArrayList<ChunkMetadata> mChunks; //index by chunk nubmer

        FileMetadata() {
            mChunks = new ArrayList<>();
        }

        public int GetNumChunks() {
            return mChunks.size();
        }
    }

    public String GetChunkLocationAtIndex(int inIndex) {
        if (inIndex > mFileMetadata.mChunks.size() || inIndex < 0) {
            System.out.println("Attempted to get chunk location at invalid index: " + inIndex);

            return null;
        }
        return mFileMetadata.mChunks.get(inIndex).mChunkLocation;
    }

    public boolean DoesChunkExistAtLocation(int inIndex, String inLocation) {
        ChunkMetadata chunkAtIndex = mFileMetadata.mChunks.get(inIndex);
        if (chunkAtIndex.GetPrimaryLocation().equalsIgnoreCase(inLocation)) {
            return true;
        }
        for (String s : chunkAtIndex.GetReplicaLocations()) {
            if (s.equalsIgnoreCase(inLocation)) {
                return true;
            }
        }
        return false;
    }

    public ChunkMetadata GetChunkDataAtIndex(int inIndex) {
        if (inIndex >= mFileMetadata.mChunks.size() || inIndex < 0) {
            System.out.println("Attempted to get chunk data at invalid index: " + inIndex);
            return null;
        }
        return mFileMetadata.mChunks.get(inIndex);
    }

    public void AddChunkAtLocation(String inLocation) {
        String chunkName = inLocation;
        mFileMetadata.mChunks.add(new ChunkMetadata(chunkName));
    }

    public void AddReplicaAtLocation(int inIndex, String inLocation) {
        mFileMetadata.mChunks.get(inIndex).mChunkReplicaLocations.add(inLocation);
    }

//    public void WriteToMessage(Message m) throws IOException {
//        m.WriteString(mName);
//        if (mIsDirectory) {
//            m.WriteInt(1);
//        } else {
//            m.WriteInt(0);
//        }
//        if (mReadLock) {
//            m.WriteInt(1);
//        } else {
//            m.WriteInt(0);
//        }
//        if (mWriteLock) {
//            m.WriteInt(1);
//        } else {
//            m.WriteInt(0);
//        }
//
//    }

//    public void ReadFromMessage(Message m) throws IOException {
//        mName = m.ReadString();
//        int bool = 0;
//        mIsDirectory = false;
//        mReadLock = false;
//        mWriteLock = false;
//
//        bool = m.ReadInt();
//        if (bool == 1) {
//            mIsDirectory = true;
//        }
//        bool = m.ReadInt();
//        if (bool == 1) {
//            mReadLock = true;
//        }
//        bool = m.ReadInt();
//        if (bool == 1) {
//            mWriteLock = true;
//        }
//    }

    public String mName;
    public boolean mIsDirectory;

    private ReadWriteLock mLock;
    //false = unlocked
    //true  = locked
    //public boolean mReadLock, mWriteLock;
    public ArrayList<FileNode> mChildren;

    //only need to make this if this is actually a file
    public FileMetadata mFileMetadata;
    
    public Boolean RequestReadLock(){
        return mLock.readLock().tryLock();
    }
    
    public Boolean RequestWriteLock(){
        return mLock.writeLock().tryLock();
    }
    
    public void ReleaseReadLock(){
        mLock.readLock().unlock();
    }
    
    public void ReleaseWriteLock(){
        mLock.writeLock().unlock();
    }

}
