/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package test;

import java.io.*;
import client.*;
import servermaster.*;
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
            testClient.CreateDir("something");
            testClient.CreateFile("something/file.txt");
            testClient.CreateFile("sldkfjsdlf/doesntexistyet.txt");
            testClient.ListFile("something");
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
        /**
     * Create a hierarchical directory structure.   
     */
    public void test1 (int dirs) {
        
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
                if(dirs - i >= 0) {
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
    
    public boolean test3(String pathname){
        try {
        testClient.DeleteFile(pathname);
        return true;
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    

}
