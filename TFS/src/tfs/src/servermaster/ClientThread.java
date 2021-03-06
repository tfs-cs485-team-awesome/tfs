/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.servermaster;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import java.util.ArrayDeque;
import java.util.ArrayList;
import tfs.util.*;

/**
 * A thread that handles all client interactions on the server master
 * @author laurencewong
 */
public class ClientThread extends Thread {

	private volatile boolean isRunning = true;

	MySocket mSocket;
	ArrayDeque<Message> mPendingMessages;
	FileNode mFileRoot;
	ServerMaster mMaster;

	/**
	 * Initializes the containers that need to be instantiated
	 */
	public final void Init() {
		mPendingMessages = new ArrayDeque<>();
	}

	/**
	 * Sends the ID that the server master chose for this client to the actual
	 * client
	 */
	public final void SendID() {
		Message sendID = new Message();
		sendID.WriteString("setid");
		sendID.WriteString(mSocket.GetID());
		sendID.SetSocket(mSocket);
		mPendingMessages.push(sendID);
	}

	/**
	 * Returns the ID of this client
	 * @return the id of this client
	 */
	public final String GetID() {
		return mSocket.GetID();
	}

	/**
	 * Creates a new client thread
	 * @param inSocket The socket that the client is on
	 * @param inMaster The server master that this clientThread is on
	 * @param inID The ID of the client
	 * @param inNode The root of the file tree
	 */
	public ClientThread(MySocket inSocket, ServerMaster inMaster, String inID, FileNode inNode) {
		mSocket = inSocket;
		mSocket.SetID(inID);
		mFileRoot = inNode;
		mMaster = inMaster;
		Init();
		SendID();
	}

	/**
	 * Closes this socket and stops the thread
	 * @throws IOException 
	 */
	public void Close() throws IOException {
		mSocket.close();
		isRunning = false;
	}

	/**
	 * The run loop of the client.  Reads in data from the socket and stores
	 * it in a message.
	 */
	@Override
	public void run() {
		System.out.println("Running client thread");
		while (isRunning) {
			try {
				if (mSocket.hasData()) {
					Message messageReceived = new Message(mSocket.ReadBytes());
					Message messageSending = ParseClientInput(messageReceived);
					messageSending.SetSocket(mSocket);
					mPendingMessages.push(messageSending);
				}
				if (!mPendingMessages.isEmpty()) {
					mPendingMessages.pop().Send();
				}
				Thread.sleep(50);
			} catch (IOException ioe) {
				System.out.println(ioe.getMessage());
			} catch (InterruptedException ie) {
				System.out.println(ie.getMessage());
			}
		}
		this.interrupt();
	}

	/**
	 * Stores the absolute path names of every file under the path given into
	 * the m
	 * @param path The absolute path to list the files under
	 * @param m The message that the names are stored in
	 */
	public void GetFilesUnderPath(String path, Message m) {
		FileNode topNode = GetAtPath(path);
		if (topNode == null) {
			System.out.println("Path does not exist");
			m.WriteDebugStatement("Path does not exist");
			return;
		}
		ArrayList<String> totalPath = new ArrayList<>();
		m.WriteString("SM-GetFilesUnderPathResponse");
		totalPath.add(path);
		for (FileNode fn : topNode.mChildren) {
			RecurseGetFilesUnderPath(fn, totalPath, path, m);
		}
		m.WriteInt(totalPath.size());
		for (String s : totalPath) {
			m.WriteString(s);
		}
	}

	/**
	 * Recursively reads the names of the files under curNOde and stores them in totalPaths.
	 * @param curNode The current node that the recursive method 
	 * @param totalPaths The list of absolute paths up until this point
	 * @param parentPath The name of the parent
	 * @param m The message to write the names to
	 */
	public void RecurseGetFilesUnderPath(FileNode curNode, ArrayList<String> totalPaths, String parentPath, Message m) {
		if (curNode.mIsDirectory) {
			totalPaths.add(parentPath + "/" + curNode.mName);
			System.out.println(parentPath + "/" + curNode.mName);
			for (FileNode fn : curNode.mChildren) {
				RecurseGetFilesUnderPath(fn, totalPaths, parentPath + "/" + curNode.mName, m);
			}
		}
	}

	/**
	 * Recursively deletes all the directories and files under a path starting
	 * at the filenode given and stores them in the arraylists
	 * @param path The path to recursively delete under
	 * @param file The filenode to start at
	 * @param allDirs All the directories that have been deleted
	 * @param allFiles All the files that have been deleted
	 * @param m The message to write to
	 * @return Whether or not the delete was successful
	 */
	public Boolean RecursiveDeleteDirectory(String path, FileNode file, ArrayList<String> allDirs, ArrayList<String> allFiles, Message m) {
		// delete all children in the directory
		Boolean success;
		for (FileNode child : file.mChildren) {
			if (child.RequestWriteLock()) {
				try {
					if (child.mIsDirectory) {
						allDirs.add(path + "/" + child.mName);
						success = RecursiveDeleteDirectory(path + "/" + child.mName, child, allDirs, allFiles, m);
					} else {
						allFiles.add(path + "/" + child.mName);
						success = true;
					}
				} finally {
					child.ReleaseWriteLock();
				}
				if (!success) {
					return false;
				}
			} else {
				System.out.println("Child directory is locked, cancelling command");
				m.WriteDebugStatement("Child directory is locked, cancelling command");
				return false;
			}
		}
		return true;
	}

	/**
	 * Gets the filenode for a given filepath
	 * @param filePath The filepath for which a node will be gotten from
	 * @return The filenode that is at the filepath given
	 */
	public FileNode GetAtPath(String filePath) {
		// check for the initial "/"
		if (filePath.indexOf("/") != 0) {
			filePath = "/" + filePath;
		}
		String[] filePathTokens = filePath.split("/");
		FileNode curFile = mFileRoot;
		// iterate through each directory in the path
		for (int i = 1; i < filePathTokens.length; ++i) {
			String dir = filePathTokens[i];
			boolean dirExists = false;
			if (!curFile.mIsDirectory) {
				return null;
			}
			// if a match is found, set current file to the match
			for (FileNode file : curFile.mChildren) {
				if (file.mName.equalsIgnoreCase(dir)) {
					curFile = file;
					dirExists = true;
					break;
				}
			}
			if (!dirExists) {
				return null;
			}
		}
		// return the successfully retrieved file node
		return curFile;
	}

	public Boolean ReadLockPath(String filePath) {
		return LockPath(filePath, false);
	}

	public Boolean WriteLockPath(String filePath) {
		return LockPath(filePath, true);
	}

	/**
	 * Attempts to lock everything under the path given
	 * @param filePath The path to lock everything under
	 * @param isWriteLock Whether or not this is trying to get a readlock or
	 * a write lock
	 * @return Whether or not the lock was successful
	 */
	public Boolean LockPath(String filePath, Boolean isWriteLock) {
		// check for the initial "/"
		if (filePath.indexOf("/") != 0) {
			filePath = "/" + filePath;
		}
		String[] filePathTokens = filePath.split("/");
		FileNode curFile = mFileRoot;
		// iterate through each directory in the path
		for (int i = 1; i < filePathTokens.length; ++i) {
			String dir = filePathTokens[i];
			boolean dirExists = false;
			if (!curFile.mIsDirectory) {
				if (isWriteLock) {
					return curFile.RequestWriteLock();
				} else {
					return curFile.RequestReadLock();
				}
			}
			// if a match is found, set current file to the match
			for (FileNode file : curFile.mChildren) {
				if (file.mName.equalsIgnoreCase(dir)) {
					curFile = file;
					dirExists = true;
					Boolean success;
					if (isWriteLock) {
						success = curFile.RequestWriteLock();
					} else {
						success = curFile.RequestReadLock();
					}
					if (!success) {
						return false;
					}
					break;
				}
			}
			if (!dirExists) {
				return true;
			}
		}
		return true;
	}

	public void UnlockReadPath(String filePath) {
		UnlockPath(filePath, false);
	}

	public void UnlockWritePath(String filePath) {
		UnlockPath(filePath, true);
	}

	/**
	 * Attempts to unlock the path under the file path
	 * @param filePath The path to unlock under
	 * @param isWriteLock Whether or not this is unlocking the write or read
	 * lock
	 */
	public void UnlockPath(String filePath, Boolean isWriteLock) {
		// check for the initial "/"
		if (filePath.indexOf("/") != 0) {
			filePath = "/" + filePath;
		}
		String[] filePathTokens = filePath.split("/");
		FileNode curFile = mFileRoot;
		// iterate through each directory in the path
		for (int i = 1; i < filePathTokens.length; ++i) {
			String dir = filePathTokens[i];
			boolean dirExists = false;
			if (!curFile.mIsDirectory) {
				if (isWriteLock) {
					curFile.ReleaseWriteLock();
					return;
				} else {
					curFile.ReleaseReadLock();
					return;
				}
			}
			// if a match is found, set current file to the match
			for (FileNode file : curFile.mChildren) {
				if (file.mName.equalsIgnoreCase(dir)) {
					curFile = file;
					dirExists = true;
					if (isWriteLock) {
						curFile.ReleaseWriteLock();
					} else {
						curFile.ReleaseReadLock();
					}
					break;
				}
			}
			if (!dirExists) {
				return;
			}
		}
	}

	/**
	 * Parses the client's input to this server master
	 * @param m The message to parse
	 * @return The message to reply back to the client
	 */
	public Message ParseClientInput(Message m) {
		Message outputToClient = new Message();

		String command = m.ReadString();
		System.out.println("Server Received: " + command);
		outputToClient.WriteDebugStatement("Server Received " + command);
		switch (command.toLowerCase()) {
			case "mkdir":
				CreateNewDir(m.ReadString(), outputToClient);
				break;
			case "touch":
				CreateNewFile(m.ReadString(), outputToClient);
				break;
			case "rm":
				DeleteDirectory(m.ReadString(), outputToClient);
				break;
			case "ls":
				ListFiles(m.ReadString(), outputToClient);
				break;
			case "read":
				ReadFile(m.ReadString(), outputToClient);
				break;
			case "readhaystack":
				ReadHaystack(m.ReadString(), m.ReadInt(), outputToClient);
				break;
			case "write":
				WriteFile(m.ReadString(), m.ReadInt(), outputToClient);
				break;
			case "append":
				AppendFile(m.ReadString(), outputToClient);
				break;
			case "stream":
				AppendFile(m.ReadString(), outputToClient);
				break;
			case "getfilesunderpath":
				GetFilesUnderPath(m.ReadString(), outputToClient);
				break;
			case "logicalfilecount":
				LogicalFileCount(m.ReadString(), outputToClient);
				break;

		}
		System.out.println("Finished client input");
		outputToClient.WriteDebugStatement("Finished client input");
		return outputToClient;
	}

	/**
	 * Returns a file from the haystack given an index into the file
	 * @param fileName The name of the haystack
	 * @param inIndex The index of the file to get
	 * @param output The message to output
	 */
	public void ReadHaystack(String fileName, int inIndex, Message output) {
		System.out.println("Sending back info for readfile: " + fileName);
		FileNode fileNode = GetAtPath(fileName);
		if (fileNode == null) {
			System.out.println("File does not exist");
			output.WriteDebugStatement("File does not exist");
		}
		if (ReadLockPath(fileName)) {
			try {
				//TODO error check if the file.GetChunkDataAtIndex return null
				FileNode.ChunkMetadata chunk = fileNode.GetChunkDataAtIndex(0);
				output.WriteString("sm-readhaystackresponse");
				output.WriteString(fileName);
				output.WriteInt(inIndex);
				output.WriteString(chunk.GetPrimaryLocation());
				output.WriteInt(chunk.GetReplicaLocations().size());
				for (String s : chunk.GetReplicaLocations()) {
					output.WriteString(s);
				}
			} finally {
				UnlockReadPath(fileName);
			}
		}
	}

	/**
	 * Creates a new directory in the file structure.  Locks all of the directories
	 * up until the directory to be made before writing the new directory in
	 * @param name The absolute path of the directory to make
	 * @param m The message to write to
	 */
	public void CreateNewDir(String name, Message m) {
		// check for the initial "/"
		int firstIndex = name.indexOf("/");
		if (firstIndex != 0) {
			name = "/" + name;
		}
		// check if the given directory already exists
		if (GetAtPath(name) != null) {
			System.out.println("Directory already exists");
			m.WriteDebugStatement("Directory already exists");
			return;
		}
		// retrieve index of the last "/"
		int lastIndex = name.lastIndexOf("/");
		if (lastIndex < 0) {
			System.out.println("Invalid name");
			m.WriteDebugStatement("Invalid name");
			return;
		}
		// default parent node to the root node
		FileNode parentNode = mFileRoot;
		// set parent node to the parent directory
		if (lastIndex > 1) {
			String parent = name.substring(0, lastIndex);
			parentNode = GetAtPath(parent);
			if (parentNode == null || !parentNode.mIsDirectory) {
				System.out.println("Parent directory does not exist");
				m.WriteDebugStatement("Parent directory does not exist");
				return;
			}
		}
		// check for locks in the parent node
		if (WriteLockPath(name)) {
			try {
				// save file structure
				mMaster.SaveFileStructure(true, name);
				// create new directory
				System.out.println("Creating new dir " + name);
				m.WriteDebugStatement("Creating new dir " + name);
				FileNode newDir = new FileNode(false);
				newDir.mIsDirectory = true;
				newDir.mName = name.substring(lastIndex + 1, name.length());
				parentNode.mChildren.add(newDir);
				// lock newly created directory
				newDir.RequestWriteLock();
				System.out.println("Finished creating new dir");
				m.WriteDebugStatement("Finished creating new dir");
			} finally {
				UnlockWritePath(name);
			}
		} else {
			System.out.println("Parent directory is locked, cancelling command");
			m.WriteDebugStatement("Parent directory is locked, cancelling command");
		}
	}

	/**
	 * Helper function to call CreateNewFile with a default replica count of 2.
	 * @param name The absolute path of the file to create
	 * @param m The message to write output to
	 */
	public void CreateNewFile(String name, Message m) {
		CreateNewFile(name, 2, m);
	}

	/**
	 * Creates a new file given an absolute path and the number of replicas
	 * the file should have.
	 * @param name The absolute path of the file
	 * @param numReplicas The number of replicas this file should have
	 * @param m The message to write output to
	 */
	public void CreateNewFile(String name, int numReplicas, Message m) {
		// check for the initial "/"
		int firstIndex = name.indexOf("/");
		if (firstIndex != 0) {
			name = "/" + name;
		}
		// check if the given file already exists
		if (GetAtPath(name) != null) {
			System.out.println("File already exists");
			m.WriteDebugStatement("File already exists");
			return;
		}
		// retrieve index of the last "/"
		int lastIndex = name.lastIndexOf("/");
		if (lastIndex < 0) {
			System.out.println("Invalid name");
			m.WriteDebugStatement("Invalid name");
			return;
		}
		// default parent node to the root node
		FileNode parentNode = mFileRoot;
		// set parent node to the parent directory
		if (lastIndex > 1) {
			String parent = name.substring(0, lastIndex);
			parentNode = GetAtPath(parent);
			if (parentNode == null) {
				System.out.println("Parent directory does not exist");
				m.WriteDebugStatement("Parent directory does not exist");
				return;
			}
		}
		//make sure more chunk servers exist than the numReplicas requested
		synchronized (mMaster.mChunkServers) {
			if (mMaster.mChunkServers.size() - 1 < numReplicas) {
				m.WriteDebugStatement("Not enough chunk servers");
				return;
			}
		}
		// verify if the parent node is a directory
		if (!parentNode.mIsDirectory) {
			System.out.println("Parent is not a directory");
			m.WriteDebugStatement("Parent is not a directory");
			return;
		}
		// check for locks in the parent node
		if (WriteLockPath(name)) {
			try {
				// save file structure
				mMaster.SaveFileStructure(false, name);
				// create new file
				System.out.println("Creating new file " + name);
				m.WriteDebugStatement("Creating new file " + name);
				FileNode newFile = new FileNode(true);
				newFile.mIsDirectory = false;
				newFile.mName = name.substring(lastIndex + 1, name.length());
				parentNode.mChildren.add(newFile);
				// lock newly created file
				newFile.RequestWriteLock();
				System.out.println("Finished creating new dir");
				m.WriteDebugStatement("Finished creating new dir");
				//make a random chunk server the location of this new file
				mMaster.AssignChunkServerToFile(name, numReplicas);
			} finally {
				UnlockWritePath(name);
			}
		} else {
			System.out.println("Parent directory is locked, cancelling command");
			m.WriteDebugStatement("Parent directory is locked, cancelling command");
		}
	}

	/**
	 * Checks if a file exists and, if it does, returns the location of the
	 * chunk and its replicas to the requesting client
	 * @param fileName The absolute path of the file
	 * @param output The message to write output to
	 */
	public void AppendFile(String fileName, Message output) {
		FileNode file = GetAtPath(fileName);
		if (file == null) {
			CreateNewFile(fileName, 2, output);
			file = GetAtPath(fileName);
			if (file == null) { // create new file failed for some reason
				//output.WriteDebugStatement("Num replicas requested: " + numReplicas + " num chunkserver: " + mMaster.mChunkServers.size());
				return;
			}
		}
		if (WriteLockPath(fileName)) {
			try {
				FileNode.ChunkMetadata chunk = file.GetChunkDataAtIndex(0);
				output.WriteString("sm-appendresponse");
				output.WriteString(fileName);
				output.WriteString(chunk.GetPrimaryLocation());
				output.WriteInt(chunk.GetReplicaLocations().size());
				for (String s : chunk.GetReplicaLocations()) {
					output.WriteString(s);
				}

				Message toChunkServer = new Message();
				toChunkServer.WriteString("sm-makeprimary");
				toChunkServer.WriteString(fileName);
				toChunkServer.SetSocket(mMaster.GetChunkSocket(chunk.GetPrimaryLocation()));
				//toChunkServer.Send();
				mPendingMessages.push(toChunkServer);
			} finally {
				UnlockWritePath(fileName);
			}
		} else {
			System.out.println("File is locked, cancelling append command");
			output.WriteDebugStatement("File is locked, cancelling append command");
		}
	}

	/**
	 * Checks if a file exists and, if it does not, creates a new file and returns
	 * the location of the new file and its replicas to the requesting client.
	 * @param fileName The absolute path of the file
	 * @param numReplicas The number of replicas that is being requested by
	 * the client
	 * @param output The message to write output to
	 */
	public void WriteFile(String fileName, int numReplicas, Message output) {

		FileNode file = GetAtPath(fileName);
		if (file != null) {
			System.out.println("File already exists: " + fileName);
			output.WriteDebugStatement("File already exists: " + fileName);
			return;
		}
		CreateNewFile(fileName, numReplicas, output);
		file = GetAtPath(fileName);
		if (file == null) {
			//was not able to create file
			output.WriteDebugStatement("Too many replicas requested");
			return;
		}
		if (WriteLockPath(fileName)) {
			try {
				FileNode.ChunkMetadata chunk = file.GetChunkDataAtIndex(0);
				output.WriteString("sm-writeresponse");
				output.WriteString(fileName);
				output.WriteString(chunk.GetPrimaryLocation());
				output.WriteInt(chunk.GetReplicaLocations().size());
				for (String s : chunk.GetReplicaLocations()) {
					output.WriteString(s);
				}
				/*
				 Message toChunkServer = new Message();
				 MySocket chunkServerSocket = mMaster.GetChunkSocket(file.GetChunkLocationAtIndex(0));
				 toChunkServer.SetSocket(chunkServerSocket);
				 toChunkServer.WriteDebugStatement("Making primary");
				 toChunkServer.WriteString("sm-makeprimary");
				 mPendingMessages.push(toChunkServer);
				 */
			} finally {
				UnlockWritePath(fileName);
			}
		} else {
			System.out.println("File is locked, cancelling write command");
			output.WriteDebugStatement("File is locked, cancelling write command");
		}
	}

	/**
	 * Checks if a file exists and is indeed a file and not a directory.  Then
	 * returns the name of the file and the location of the chunk to the client
	 * @param fileName The name of the file
	 * @param output The message to write output to
	 */
	public void LogicalFileCount(String fileName, Message output) {
		FileNode fileNode = GetAtPath(fileName);
		if (fileNode == null) {
			System.out.println("File does not exist");
			output.WriteDebugStatement("File does not exist");
		} else if (fileNode.mIsDirectory) {
			System.out.println("Given path is not a file");
			output.WriteDebugStatement("Given path is not a file");
		} else {
			FileNode.ChunkMetadata chunk = fileNode.GetChunkDataAtIndex(0);
			output.WriteString("sm-logicalfilecountresponse");
			output.WriteString(fileName);
			output.WriteString(chunk.GetPrimaryLocation());
		}
	}

	/**
	 * Checks if a file exists and returns the file information to the client
	 * @param fileName The name of the file to read
	 * @param output The message to write output to
	 */
	public void ReadFile(String fileName, Message output) {
		System.out.println("Sending back info for readfile: " + fileName);
		FileNode fileNode = GetAtPath(fileName);
		if (fileNode == null) {
			System.out.println("File does not exist");
			output.WriteDebugStatement("File does not exist");
		}
		if (ReadLockPath(fileName)) {
			try {
				//TODO error check if the file.GetChunkDataAtIndex return null
				FileNode.ChunkMetadata chunk = fileNode.GetChunkDataAtIndex(0);
				output.WriteString("sm-readfileresponse");
				output.WriteString(fileName);
				output.WriteString(chunk.GetPrimaryLocation());
				output.WriteInt(chunk.GetReplicaLocations().size());
				for (String s : chunk.GetReplicaLocations()) {
					output.WriteString(s);
				}
			} finally {
				UnlockReadPath(fileName);
			}
		}
	}
        /**
         * Checks if the given path points to a directory. If it is, and if there are any files in the directory, checks for locks in the file
         * directory. If there are no locks, all files in the directory are printed.
         * @param filePath
         * @param m 
         */
	public void ListFiles(String filePath, Message m) {
		System.out.println("Listing files for path: " + filePath);
		m.WriteDebugStatement("Listing files for path: " + filePath);
		FileNode fileDir = GetAtPath(filePath);
		if (fileDir == null) {
			System.out.println("No directory named " + filePath + " exists");
			m.WriteDebugStatement("No directory named " + filePath + " exists");
			return;
		}
		if (!fileDir.mIsDirectory) {
			System.out.println("Path is not a directory");
			m.WriteDebugStatement("Path is not a directory");
			return;
		}
		if (fileDir.mChildren.isEmpty()) {
			System.out.println("No files in directory " + filePath);
			m.WriteDebugStatement("No files in directory " + filePath);
			return;
		}
		// check for locks in file directory
		if (ReadLockPath(filePath)) {
			try {
				for (FileNode file : fileDir.mChildren) {
					System.out.println(file.mName);
					m.WriteDebugStatement(file.mName);
				}
			} finally {
				UnlockReadPath(filePath);
			}
		} else {
			System.out.println("Directory is locked, cancelling command");
			m.WriteDebugStatement("Directory is locked, cancelling command");
		}
	}
        /**
         * Checks if the directory given is the root. If not, and if it exists, the directory is checked for any locks on the directory and
         * on its parent directory. If there are no locks, the directory is deleted.
         * @param path
         * @param m 
         */
	public void DeleteDirectory(String path, Message m) {
		// check for the first "/"
		int firstIndex = path.indexOf("/");
		if (firstIndex != 0) {
			path = "/" + path;
		}
		// check if the path is the root
		if (path.equals("/")) {
			System.out.println("Cannot delete root folder");
			m.WriteDebugStatement("Cannot delete root folder");
			return;
		}
		// check if the given path exists
		FileNode file = GetAtPath(path);
		if (file == null) {
			System.out.println("Path does not exist");
			m.WriteDebugStatement("Path does not exist");
			return;
		}
		// retrieve index of the last "/"
		int lastIndex = path.lastIndexOf("/");
		if (lastIndex < 0) {
			System.out.println("Invalid name");
			m.WriteDebugStatement("Invalid name");
			return;
		}
		// default parent node to the root node
		FileNode parentNode = mFileRoot;
		// set parent node to the parent directory
		if (lastIndex > 1) {
			String parent = path.substring(0, lastIndex);
			parentNode = GetAtPath(parent);
			if (parentNode == null) {
				System.out.println("Parent directory does not exist");
				m.WriteDebugStatement("Parent directory does not exist");
				return;
			}
		}
		// check for locks in the parent node
		// check for locks in the current node
		if (WriteLockPath(path)) {
			try {
				// check if the path is a file
				if (!file.mIsDirectory) {
					DeleteFile(path, m);
					return;
				}
				ArrayList<String> allFiles = new ArrayList<String>();
				ArrayList<String> allDirs = new ArrayList<String>();
				allDirs.add(path);
				// find all children in the directory
				Boolean success = true;
				for (FileNode child : file.mChildren) {
					if (child.mIsDirectory) {
						allDirs.add(path + "/" + child.mName);
						success = RecursiveDeleteDirectory(path + "/" + child.mName, child, allDirs, allFiles, m);
					} else {
						allFiles.add(path + "/" + child.mName);
						success = true;
					}
				}
				if (!success) {
					System.out.println("Directory is in use, cancelling delete");
					m.WriteDebugStatement("Directory is in use, cancelling delete");
				} else {
					// delete files from file structure
					for (String fileName : allFiles) {
						DeleteFile(fileName, m);
					}
					// delete directories from file structure
					for (String dir : allDirs) {
						DeleteFromFileStructure(true, dir);
						// delete directory
						System.out.println("Deleting directory " + dir);
						m.WriteDebugStatement("Deleting directory " + dir);
					}
					// remove link from parent node to this directory
					parentNode.mChildren.remove(file);
				}
			} finally {
				UnlockWritePath(path);
			}
		} else {
			System.out.println("File directory is locked, cancelling command");
			m.WriteDebugStatement("File directory is locked, cancelling command");
		}
	}
        /**
         * Deletes the file at the given filepath, sends a message to the chunkservers to delete all replicas of the file
         * @param filePath
         * @param m 
         */
	public void DeleteFile(String filePath, Message m) {
		// retrieve file node
		FileNode file = GetAtPath(filePath);
		// retrieve index of the last "/"
		int lastIndex = filePath.lastIndexOf("/");
		// default parent node to the root node
		FileNode parentNode = mFileRoot;
		// set parent node to the parent directory
		if (lastIndex > 1) {
			String parent = filePath.substring(0, lastIndex);
			parentNode = GetAtPath(parent);
		}
		// delete file from file structure
		DeleteFromFileStructure(false, filePath);
		// delete file
		System.out.println("Deleting file " + filePath);
		m.WriteDebugStatement("Deleting file " + filePath);
		parentNode.mChildren.remove(file);
		FileNode.ChunkMetadata chunk = file.GetChunkDataAtIndex(0);
		Message toChunkServer = new Message();
		toChunkServer.WriteString("sm-deletefile");
		toChunkServer.WriteString(filePath);
		toChunkServer.SetSocket(mMaster.GetChunkSocket(chunk.GetPrimaryLocation()));
		mPendingMessages.push(toChunkServer);
		for (int i = 0; i < chunk.GetReplicaLocations().size(); i++) {
			chunk = file.GetChunkDataAtIndex(0);
			toChunkServer = new Message();
			toChunkServer.WriteString("sm-deletefile");
			toChunkServer.WriteString(filePath);
			toChunkServer.SetSocket(mMaster.GetChunkSocket(chunk.GetReplicaLocations().get(i)));
			mPendingMessages.push(toChunkServer);
		}
	}
        /**
         * Finds the file at the given pathname, checks if it is a directory or a file. If it is a file, the file is located, and if it exists,
         * it is deleted from the file structure.
         * @param isDirectory true if the given path points to a directory, false if it points to a file
         * @param name pathname of the node to delete
         */
	public void DeleteFromFileStructure(Boolean isDirectory, String name) {
		String lineToDelete;
		if (isDirectory) {
			lineToDelete = "DIRECTORY " + name;
		} else {
			lineToDelete = "FILE " + name;
		}
		File file = new File("SYSTEM_LOG.txt");
		if (!file.exists() || !file.isFile()) {
			System.out.println("Unable to delete from TFS file structure");
			return;
		}
		try {
			File tempFile = new File(file.getAbsolutePath() + ".tmp");
			BufferedReader br = new BufferedReader(new FileReader(file));
			PrintWriter pw = new PrintWriter(new FileWriter(tempFile));

			String line;
			while ((line = br.readLine()) != null) {
				if (!line.trim().equals(lineToDelete)) {
					pw.println(line);
					pw.flush();
				}
			}
			pw.close();
			br.close();
			if (!file.delete()) {
				System.out.println("Could not delete file");
				return;
			}
			if (!tempFile.renameTo(file)) {
				System.out.println("Could not rename file");
			}
		} catch (IOException ie) {
			System.out.println("Failed to delete file from the TFS file structure");
		}
	}

}
