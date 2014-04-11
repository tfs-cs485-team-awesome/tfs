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

/**
 *
 * @author Kevin
 */
public class TestCases {
    public static void main(String[] args) {
        Client testClient = new Client("localhost:6789");
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
}
