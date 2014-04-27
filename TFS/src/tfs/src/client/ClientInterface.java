/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tfs.src.client;


import java.io.*;
import tfs.util.FileNode;
/**
 *
 * @author laurencewong
 */
public interface ClientInterface {
    /**
     * Adds message to the deque to create a file.
     * @param fileName
     * @throws IOException 
     */
    public void CreateFile(String fileName) throws IOException;
    /**
     * Adds message to deque to create a directory.
     * @param dirName
     * @throws IOException 
     */
    public void CreateDir(String dirName) throws IOException;
    /**
     * Adds message to the deque to delete a file.
     * @param fileName
     * @throws IOException 
     */
    public void DeleteFile(String fileName) throws IOException;
    /**
     * Adds message to the deque to list all files.
     * @param path
     * @throws IOException 
     */
    public void ListFile(String path) throws IOException;
    /**
     * Adds message to the deque to list all files.
     * @param path
     * @throws IOException 
     */
    public void GetListFile(String path) throws IOException;
    /**
     * Waits in a busy loop until files have been found, then returns an array of the files.
     * @return String array of the files.
     * @throws IOException
     * @throws InterruptedException 
     */
    public String[] GetListFile() throws IOException, InterruptedException;
    /**
     * Adds message to the deque to read file at the remote location on the server, into the local file.
     * @param remotefilename name of the file on the server to be read from
     * @param localfilename name of the local file to be read into
     * @throws IOException 
     */
    public void ReadFile(String remotefilename, String localfilename) throws IOException;
    /**
     * Adds message to the deque to get the file located at the given pathname.
     * @param path
     * @throws IOException 
     */
    public void GetAtFilePath(String path) throws IOException;
    /**
     * Enters busy loop until file is available, then returns the filenode at the given path.
     * @return Filenode at given pathname
     * @throws IOException
     * @throws InterruptedException 
     */
    public FileNode GetAtFilePath() throws IOException, InterruptedException;
    /**
     * Adds message to the deque to write data from a local file into a file on the server, with a given number of copies.
     * @param localfilename local file to write data from
     * @param remotefilename file on server to write data to
     * @param numReplicas number of copies of file to create
     * @throws IOException 
     */
    public void WriteFile(String localfilename, String remotefilename, int numReplicas) throws IOException;
    /**
     * Adds message to the deque to append data from local file to the file on the server
     * @param localfilename path for local file
     * @param remotefilename path for file on server
     * @throws IOException 
     */
    public void AppendFile(String localfilename, String remotefilename) throws IOException;
    /**
     * Adds message to the deque to count the number of files under the given directory path
     * @param remotename
     * @throws IOException 
     */
    public void CountFiles(String remotename) throws IOException;
    /**
     * Writes given data to a local file
     * @param fileName local filename to write to
     * @param data data to write
     * @throws IOException 
     */
    public void WriteLocalFile(String fileName, byte[] data) throws IOException;
}
