// File:    CommandLineIO.scala
// Author:  Henry Feild
// Date:    12-Sep-2011
// Copyright 2013 Henry Feild
// License: Revised BSD -- see LICENSE in the root directory.
 
package edu.umass.ciir.tqgraph.util

/**
 * Provides functionality to ease interactions with a command line interface.
 *
 * @author hfeild
 */
object CommandLineIO 
{
    def exitNice( usage:String )
    {
        Console.err.println( usage )
        System.exit(1)
    }
}
