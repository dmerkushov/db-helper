/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ru.dmerkushov.confighelper;

/**
 *
 * @author shandr
 */
public class ConfigHelperException extends Exception {

    /**
     * Creates a new instance of <code>ConfigHelperException</code> without detail message.
     */
    public ConfigHelperException() {
    }


    /**
     * Constructs an instance of <code>ConfigHelperException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public ConfigHelperException(String msg) {
        super(msg);
    }


    /**
     * Constructs an instance of <code>ConfigHelperException</code> with the specified cause.
	 * @param cause the cause (which is saved for later retrieval by the Exception.getCause() method). (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public ConfigHelperException(Throwable cause) {
        super(cause);
    }


    /**
     * Constructs an instance of <code>ConfigHelperException</code> with the specified detail message and cause.
     * @param msg the detail message.
	 * @param cause the cause (which is saved for later retrieval by the Exception.getCause() method). (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public ConfigHelperException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
