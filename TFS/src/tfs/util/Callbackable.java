/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tfs.util;

/**
 * This interface contains function Callback
 * @author laurencewong
 */
public interface Callbackable {
    /**
     * This function closes given socket
     * @param inParameter The name of the socket to close
     */
    void Callback(String inParameter);
}
