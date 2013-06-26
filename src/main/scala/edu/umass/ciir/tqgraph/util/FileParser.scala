// File:    FileParser.java
// Author:  Henry Feild
// Date:    Aug 26, 2011
// Copyright 2013 Henry Feild
// License: Revised BSD -- see LICENSE in the root directory.

package edu.umass.ciir.tqgraph.util

import java.nio.charset.Charset
import scala.io.Codec
import java.io.InputStream
import java.io.BufferedReader
import java.util.Scanner

/**
 * Provides functions to assist with parsing files, e.g., processing each
 * line and cumulating data across lines.
 *
 * @author hfeild
 */
object FileParser 
{

    /**
     * Parses a file, specified by the given file name, and processes each 
     * line according to <code>lineParser</code>. An example use would be to
     * call this function to read the file in as a list of strings:
     * 
     *   var myList:List[String] = parseFile[List[String]]( 
     *      "myFile.txt", List(), (s:String, l:List[String]) => (l:::s) )
     *      
     * @param filename  The name of the file to read in.
     * @param starter   The initial structure to use, e.g. an empty list or map.
     * @param lineParser The function to run on each line of the file.
     * 
     * @return A structure of the file, as built by the <code>lineParser</code> 
     *      function.
     */
    def parseLatinFile[T]( filename:String, starter:T, 
                          lineParser:(String,T)=>T ):T =
    {
        parseFile[T]( filename, starter, lineParser, scala.io.Codec.ISO8859 )
    }

    def parseLatinFile2( filename:String, lineParser:(String)=>Unit ) {
        parseFile2( filename, lineParser, scala.io.Codec.ISO8859 )
    }
    


    def parseFile[T]( filename:String, starter:T, lineParser:(String,T)=>T,
            charset:scala.io.Codec = scala.io.Codec.UTF8 ):T =
            // charset:Charset = scala.io.Codec.UTF8 ):T =
    {
        var structure:T = starter 
        
        val source = ScalaFileIO.getBufferedSource( filename, charset )
        for( line <- source.getLines )
            structure = lineParser(line, structure)
        
        source.close
        
        return structure
    }


    def parseFile2( filename:String, lineParser:(String)=>Unit,
            charset:scala.io.Codec = scala.io.Codec.UTF8 ) {
        val source = ScalaFileIO.getBufferedSource( filename, charset )
        for( line <- source.getLines ) lineParser(line)
        
        source.close
    }
    
    def parseStream[T]( stream:InputStream, starter:T, 
            lineParser:(String,T)=>T):T = {
        var structure:T = starter 
        
        val source = new Scanner( stream )
        while( source.hasNextLine )
            structure = lineParser(source.nextLine(), structure)
        
        source.close
        
        return structure
    }

    def parseStream2( stream:InputStream, lineParser:(String)=>Unit) {
        val source = new Scanner( stream )
        while( source.hasNextLine ) lineParser(source.nextLine())
        
        source.close
    }
}
