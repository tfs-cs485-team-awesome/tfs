/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tfs.src.test;

import tfs.src.client.Client;
import tfs.util.FileNode;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import tfs.*;
import java.util.*;

/**
 *
 * @author Kevin
 */
public class TestCases {

    static Client testClient;

    public static void main(String[] args) {
        testClient = new Client("localhost:6789");
        try {
            testClient.ConnectToServer();
            TestCases tc = new TestCases();
            
            System.out.println("Type a test with valid parameters and press enter: ");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String input = "";
            while(true){
                input = br.readLine();
                String[] argv = input.split(" ");
                switch(argv[0]){
                    case "test1":
                        int param = Integer.parseInt(argv[1]);
                        tc.test1(param);
                        break;
                    case "test2":
                        param = Integer.parseInt(argv[2]);
                        tc.test2(argv[1], param);
                        break;
                    case "test3":
                        tc.test3(argv[1]);
                        break;
                    case "test4":
                        tc.test4(argv[1], argv[2]);
                        break;
                    case "test5":
                        tc.test5(argv[1], argv[2]);
                        break;
                    case "test6":
                        tc.test6(argv[1], argv[2]);
                        break;
                    case "test7":
                        tc.test7(argv[1]);
                        break;
                    default:
                        System.out.println("Invalid test case, please re-enter");
                        break;
                }
            }
        } catch (Exception e) {
            System.out.println("Unrecognized command, please re-enter");
        }
    }

    /**
     * Create a hierarchical directory structure.
     */
    public void test1(int dirs) {

        try {
            int i = 2;
            Queue q = new LinkedList();
            q.add("1");
            testClient.CreateDir("1");

            while (dirs - i >= 0) {
                String curstr = q.remove().toString();
                String temp = curstr + "/" + i;
                q.add(temp);
                testClient.CreateDir(temp);

                i++;
                if (dirs - i >= 0) {
                    temp = curstr + "/" + i;
                    q.add(temp);
                    testClient.CreateDir(temp);
                }
                i++;
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

    public void test2(String filePath, int numFiles) {

        try {
            String[] paths = testClient.GetListFile(filePath);
            for (String path : paths) {
                testClient.ListFile(path);
                for (int i = 1; i <= numFiles; i++) {
                    String fileName = path + "/File" + Integer.toString(i);
                    testClient.CreateFile(fileName);
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void test3(String pathName){
        try {
            testClient.DeleteFile(pathName);
        } catch(Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void test4(String localpath, String remotepath) {
        try {
            testClient.WriteFile(localpath, remotepath);
            
        } catch (IOException e) {
            System.out.println("Test 4 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void test5(String pathname, String localpath) {
        try {
            testClient.ReadFile(pathname, localpath);
        } catch (IOException e) {
            System.out.println("Test 5 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void test6(String localpath, String pathname) {
        try {
            testClient.AppendFile(localpath, pathname);
        } catch (IOException e) {
            System.out.println("Test 6 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void test7(String pathname) {
                try {
            testClient.CountFiles(pathname);
        } catch (IOException e) {
            System.out.println("Test 6 failed due to exception " + e.getMessage());
            e.printStackTrace();
        }
    }

}
