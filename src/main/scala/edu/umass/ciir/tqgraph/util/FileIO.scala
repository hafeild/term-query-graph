// File:    FileIO.java
// Author:  Henry Feild
// Date:    02-Dec-2008 (quickly adapted from Java to Scala: 20-Jun-2013).
// Note: originally supported bzip2 files.
// Copyright 2013 Henry Feild
// License: Revised BSD -- see LICENSE in the root directory.

package edu.umass.ciir.tqgraph.util;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

/**
 * Provides functions for reading and writing to files -- regular and gzipped.
 *
 * @author hfeild
 */
object FileIO {
    val APPEND = true
    val DEFAULT_OUTPUT_CODEC = "UTF8"
    val LATIN1_CODEC = "ISO8859-1"
        
    
    /**
     * <p>Creates and returns a BufferedReader for the given filename.
     * The file can be raw text, gzipped (in which case it must have a
     * '.gz' file extension), or bzipped (with a .bz2 extension).</p>
     *
     * @param filename  The filename of the file to open.
     *
     * @return A BufferedReader for the given filename.
     *
     * @throws IOException
     */
    @throws(classOf[IOException])
    def getReader( filename:String ):BufferedReader = 
        new BufferedReader( new InputStreamReader( getReaderStream( filename )))
    
    
    /**
     * <p>Creates and returns an InputStream for the given filename.
     * The file can be raw text, gzipped (in which case it must have a
     * '.gz' file extension), or bzipped (with a .bz2 extension).</p>
     *
     * @param filename  The filename of the file to open.
     *
     * @return An InputStream for the given filename.
     *
     * @throws IOException
     */
    @throws(classOf[IOException])
    def getReaderStream( filename:String ):InputStream = {
        val stream = new FileInputStream( filename )
        if( filename.endsWith(".gz") || filename.endsWith(".zip") )
            new GZIPInputStream( stream );
        else 
            stream;
    }
    
    /**
     * <p>Creates a buffered writer to write at the begining of the given file.
     * </p>
     * 
     * @param filename  The name of the file to write to.
     * 
     * @return A buffered writer to the output file.
     * 
     * @throws IOException
     */
    @throws(classOf[IOException])
    def getBufferedWriter( filename:String ):BufferedWriter =
        getBufferedWriter( filename, false, DEFAULT_OUTPUT_CODEC )
    
    
    @throws(classOf[IOException])
    def getBufferedLatinWriter( filename:String ):BufferedWriter =
        getBufferedWriter( filename, false, LATIN1_CODEC )
    
    
    /**
     * <p>Creates a buffered writer to write to the beginning of the given file
     * if the append flag is false, and to the end if the flag is true.</p>
     * 
     * @param filename  The name of the file to write to.
     * @param append    A flag specifying whether the writer should add to the
     *                  beginning (false) or end (true) of the file.
     *                  
     * @return A buffered writer to the output file.
     * 
     * @throws IOException
     */
    @throws(classOf[IOException])
    def getBufferedWriter( filename:String, append:Boolean, codec:String ):
            BufferedWriter =
        new BufferedWriter( 
            new OutputStreamWriter( 
                getOutputStream( filename, append ), codec ) )
    
    
    /**
     * <p>Creates an output stream to write at the beginning of the given file.
     * </p>
     * 
     * @param filename  The name of the file to write to.
     * 
     * @return An output stream for the output file.
     * 
     * @throws IOException
     * @throws FileNotFoundException
     */
    @throws(classOf[IOException])
    @throws(classOf[FileNotFoundException])
    def getOutputStream( filename:String ):OutputStream =
        getOutputStream( filename, false )
    
    
    
    /**
     * <p>Creates an output stream to write to the beginning of the given file
     * if the append flag is false, and to the end if the flag is true.</p>
     * 
     * @param filename  The name of the file to write to.
     * @param append    A flag specifying whether the writer should add to the
     *                  beginning (false) or end (true) of the file.
     *                  
     * @return An output stream for the output file.
     * 
     * @throws IOException
     * @throws FileNotFoundException
     */
    @throws(classOf[IOException])
    @throws(classOf[FileNotFoundException])
    def getOutputStream( filename:String, append:Boolean ):OutputStream = {
        
        val stream = new FileOutputStream( filename, append )
        if( filename.endsWith(".gz") ) 
            new GZIPOutputStream( stream )
        else
            stream
    }
    
    /**
     * <p>Creates a buffered writer to write at the end of the give file.</p>
     * 
     * @param filename  The name of the file to append to.
     * 
     * @return A buffered writer to append to the output file.
     * 
     * @throws IOException
     */
    @throws(classOf[IOException])
    def getAppendedWriter( filename:String ):BufferedWriter =
        getBufferedWriter( filename, APPEND, DEFAULT_OUTPUT_CODEC )
    

    /**
     * Recursively deletes a directory.
     * Taken from: http://www.exampledepot.com/egs/java.io/DeleteDir.html
     */
    def deleteDir( dirName:String ):Boolean = deleteDir(new File(dirName))
    
        
    def deleteDir( dir:File ):Boolean = {
        if (dir.isDirectory()) {
            val children = dir.list();
            children.foreach(child => {
                if( !deleteDir(new File(dir, child)) ) 
                    return false;
            })
        }
        // The directory is now empty so delete it
        dir.delete();
    }
}