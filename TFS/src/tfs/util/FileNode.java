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
public class FileNode {

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
     * Contains all metadata for a chunk.
     */
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
            if (mTimestamp > inTimestamp) {
                System.out.println("Trying to update chunk with older timestamp");
            }
            mTimestamp = inTimestamp;
        }

        public long GetTimestamp() {
            return mTimestamp;
        }
        /**
         * Removes chunk from given location. If chunk is primary location, one of the replica locations will be set to primary.
         * @param inLocation 
         */
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
        /**
         * Removes all replica locations for a given chunk.
         * @param inLocation 
         */
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
        /**
         * If a chunk does not have a primary location, sets the primary location as given location, otherwise adds a replica location.
         * @param inLocation 
         */
        public void SetChunkLocation(String inLocation) {
            if (mChunkLocation != null) {
                //it has a primary location
                AddReplicaLocation(inLocation);
                return;
            }
            System.out.println("Setting chunk's main location");
            mChunkLocation = inLocation;
        }
        /**
         * Adds a replica location for a chunk if that replica does not already exist.
         * @param inLocation 
         */
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
    /**
     * Checks if the given index is invalid, if not, returns the chunk location at that index
     * @param inIndex
     * @return string containing the chunk's location
     */
    public String GetChunkLocationAtIndex(int inIndex) {
        if (inIndex > mFileMetadata.mChunks.size() || inIndex < 0) {
            System.out.println("Attempted to get chunk location at invalid index: " + inIndex);

            return null;
        }
        return mFileMetadata.mChunks.get(inIndex).mChunkLocation;
    }
    /**
     * Checks if a chunk exists at a given location
     * @param inIndex chunk index
     * @param inLocation location to check for chunk
     * @return true or false if the chunk exists
     */
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
    /**
     * Gets the chunk data at the given index and returns it
     * @param inIndex
     * @return Chunk data
     */
    public ChunkMetadata GetChunkDataAtIndex(int inIndex) {
        if (inIndex >= mFileMetadata.mChunks.size() || inIndex < 0) {
            System.out.println("Attempted to get chunk data at invalid index: " + inIndex);
            return null;
        }
        return mFileMetadata.mChunks.get(inIndex);
    }
    /**
     * Adds a chunk at the given location
     * @param inLocation 
     */
    public void AddChunkAtLocation(String inLocation) {
        String chunkName = inLocation;
        mFileMetadata.mChunks.add(new ChunkMetadata(chunkName));
    }
    /**
     * Adds a replica for the given index at the given location
     * @param inIndex
     * @param inLocation 
     */
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

    private final ReentrantReadWriteLock mLock = new ReentrantReadWriteLock();
    //false = unlocked
    //true  = locked
    //public boolean mReadLock, mWriteLock;
    public ArrayList<FileNode> mChildren;

    //only need to make this if this is actually a file
    public FileMetadata mFileMetadata;

    public Boolean RequestReadLock() {
        return mLock.readLock().tryLock();
    }

    public Boolean RequestWriteLock() {
        return mLock.writeLock().tryLock();
    }

    public void ReleaseReadLock() {
        mLock.readLock().unlock();
    }

    public void ReleaseWriteLock() {
        mLock.writeLock().unlock();
    }

}
