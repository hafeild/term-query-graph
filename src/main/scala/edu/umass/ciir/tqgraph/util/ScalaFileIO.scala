// File:    ScalaFileIO.scala
// Author:  Henry Feild
// Date:    30-Aug-2011
// Copyright 2013 Henry Feild
// License: Revised BSD -- see LICENSE in the root directory.

package edu.umass.ciir.tqgraph.util

import java.nio.charset.Charset
import java.util.zip._
import java.io._
import scala.io._
/**
 * Provides basic file operations. Deals with regular, gzipped, and bzipped 
 * files. Note that gzipped and bzipped files must have the extensions
 * .gz and .bz2, respectively.
 * 
 * @author hfeild
 */
object ScalaFileIO
{
    /**
     * Returns a buffered source. Be sure to close this when you're finished.
     * This method can handle regular text files, .gz, and .bz2 files.
     * 
     * @param filename The name of the file to open.
     * @return A buffered source for <code>file</code>.
     */
    def getBufferedSource( filename:String, charset:Codec):BufferedSource = {
        //val x:scala.io.Codec = scala.io.Codec.charset2codec(charset)
        return scala.io.Source.fromInputStream( 
            FileIO.getReaderStream( filename ) )(charset)
            
    }
    
    def getBufferedLatinSource( filename:String ):BufferedSource = {
        getBufferedSource( filename, scala.io.Codec.ISO8859 )
    }
     
    
    
    /**
     * Checks if the given file exists. If the second param is
     * <code>true</code> and the file doesn't exist, the filename will be 
     * output to stderr.
     * 
     * @param filename  The filename to check.
     * @param outputMissingFilenames Whether or not to print missing filenames
     *      to stderr.
     * @return <code>true</code> if the file exists.
     */ 
    def fileExists(
            filename:String, outputMissingFilenames:Boolean=false ):Boolean =
        filesExist( List(filename), outputMissingFilenames )
    
    /**
     * Checks if all of the files in a list exist. If the second param is
     * <code>true</code>, non-existant files will be output to stderr.
     * 
     * @param filenames The list of filenames to check.
     * @param outputMissingFilenames Whether or not to print missing filenames
     *      to stderr.
     * @return <code>true</code> if all the files in the list exist.
     */ 
    def filesExist( filenames:Iterable[String], 
                   outputMissingFilenames:Boolean=false ):Boolean =
    {
        var atLeastOneFileDNE = false
        
        for( filename <- filenames )
        {
            if( !new java.io.File( filename ).exists )
            {
                atLeastOneFileDNE = true
                if( outputMissingFilenames )
                    System.err.println( 
                        "File " + filename + " dose not exist." )
            }
        }
        
        !atLeastOneFileDNE
    }

    
    /**
     * Reads the entire contents of a file in. Be careful using this with
     * large files!
     * 
     * @param filename  The file to read in.
     * @result The contents of the file.
     */
    def slurpFile( filename:String ):String =
    {
        def readAll( line:String, sb:StringBuilder ):StringBuilder =
            sb.append( line ).append("\n")
        
        FileParser.parseFile( filename, new StringBuilder(), readAll ).toString
    }
    
    
    /**
     * Reads the entire contents of a file in. Be careful using this with
     * large files!
     * 
     * @param filename  The file to read in.
     * @result The contents of the file.
     */
    def slurpStream( stream:InputStream ):String =
    {
        def readAll( line:String, sb:StringBuilder ):StringBuilder =
            sb.append( line )
        
        val reader = stream
        FileParser.parseStream( stream, new StringBuilder, readAll ).toString
    }
    
}


//
// Taken directly from:
//  http://rosettacode.org/wiki/Walk_a_directory/Recursively#Scala
// on 05-Sep-2011 with very slight modification. (hfeild)
// 
/** A wrapper around file, allowing iteration either on direct children 
     or on directory tree */
class RichFile(file: File) {
 
  def children = new Iterable[File] {
    def iterator =  // Renamed this to 'iterator'
        if (file.isDirectory) file.listFiles.toIterator else Iterator.empty;
        // changed 'file.listFiles.elements' to 'file.listFiles.toIterator'
  }
 
  def andTree : Iterable[File] = (
    Seq(file) // Changed this from Seq.single(file) (hfeild)
    ++ children.flatMap(child => new RichFile(child).andTree))
}
 
/** implicitely enrich java.io.File with methods of RichFile */
object RichFile {
  implicit def toRichFile(file: File) = new RichFile(file)
}
