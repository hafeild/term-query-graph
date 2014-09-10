// File:    ProcessGraph.scala
// Author:  Henry Feild
// Date:    13-Aug-2014
// Copyright 2014 Henry Feild
// License: Revised BSD -- see LICENSE in the root directory.

package edu.umass.ciir.tqgraph

import edu.umass.ciir.tqgraph.util.{ScalaFileIO,FileParser,FileIO,CommandLineIO}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import java.io.File

/**
 * Takes a set of labeled node edges with counts and produces two things:
 * <ul>
 *     <li>node id --> node label mapping
 *     <li>sparse rewrite adjacency matrix; rows sum to 1
 * </ul>
 *
 * @author hfeild
 */
class ProcessGraph(rewriteFile:String, nodeCountEst:Int=0) {
    import ProcessGraph._

    val nodeLabelToIDMap = new HashMap[String,Int]()
    val adjacencyMatrix = new HashMap[Int,HashMap[Int,Double]]()

    // If the caller gave a query count estimate, we can use that as a hint
    // to our hash maps.
    if( nodeCountEst > 0 ) {
        nodeLabelToIDMap.sizeHint(nodeCountEst)
        adjacencyMatrix.sizeHint(nodeCountEst)
    }

    /**
     * Adds the given query string to the query id map if it's not already
     * in there. It will be assigned the next available id.
     * 
     * @param query The query to add.
     */
    def addToMapIfNeeded(query:String) {
        if( !nodeLabelToIDMap.contains(query) )
            nodeLabelToIDMap(query) = nodeLabelToIDMap.size
    }

    /**
     * Process one line from the query reformulation file. It assumes that
     * the file has a header -- if it does not, then initialize seenHeader to
     * <code>true</code>. The reformulation file should consist of three
     * tab-delimited columns:
     * <ul>
     *     <li>query 1
     *     <li>query 2
     *     <li>frequency
     * </ul>
     * 
     * @param line          A line from the reformulation file.
     * @param seenHeader    Whether the header has been skipped. If 
     *                      <code>false</code>, then the current line is 
     *                      skipped and the return value is <code>true</code>.
     */
    def parseInputLine(line:String, seenHeader:Boolean):Boolean = {
        if( !seenHeader )
            return true

        val cols = line.split("\\t")
        val query1 = cols(Node1Col)
        val query2 = cols(Node2Col)
        val count = cols(WeightCol).toDouble

        if( query1 == query2 )
            return seenHeader
            
        addToMapIfNeeded(query1)
        addToMapIfNeeded(query2)

        if( !adjacencyMatrix.contains(nodeLabelToIDMap(query1)) )
            adjacencyMatrix(nodeLabelToIDMap(query1)) = 
                new HashMap[Int,Double]()

        adjacencyMatrix(nodeLabelToIDMap(query1))(nodeLabelToIDMap(query2)) = 
            adjacencyMatrix(nodeLabelToIDMap(query1)).getOrElse(
                nodeLabelToIDMap(query2), 0.0) + count

        seenHeader
    }

    /**
     * Normalizes the rows of the rewrite matrix so that the outgoing edges
     * of each query sums to 1.
     */
    def normalizeRows(){
        adjacencyMatrix.values.foreach{nodes =>
            val sum = nodes.values.sum.toDouble
            nodes.foreach{case(n,v) =>
                nodes(n) = v / sum 
            }
        }
    }

    /**
     * Reads in and parses the rewrite file, creating a normalized, sparse 
     * re-write matrix,
     */
    def generateSparseAdjacencyMatrix() {
        FileParser.parseLatinFile(rewriteFile, false, parseInputLine)
        normalizeRows()
    }

    /**
     * Writes the sparse re-write matrix to file. The output format is:
     * [src query id]\t[dest query1 id]\t[prob1]\t[dest query2 id]\t[prob1]...
     * e.g.: 
     * <ul>
     *     1  2  0.5  3  0.25  4  0.25
     * </ul>
     * This represents the query with id 1 being reformulated as the query with
     * id 2 50% of the time, reformulated as the query with id 3 25% of the
     * time, and reformulated as the query with id 4 25% of the time. The 
     * probabilities sum to 1 for each row.
     * 
     * @param filename  The name of the file to write the sparse matrix to.
     */
    def writeMatrix(filename:String) {
        val file = FileIO.getBufferedLatinWriter(filename)
        adjacencyMatrix.foreach{case(nid, nodes) =>
            file.write(nid.toString)
            nodes.foreach{case(nid2,count) =>
                file.write("\t")
                file.write(nid2.toString)
                file.write("\t")
                file.write(count.toString)
            }
            file.write("\n")
        }
        file.close()
    }

    /**
     * Writes the query mapping to file. The format is:
     * [query text]\t[query id]
     * E.g.,
     * <ul>
     *     free books  1
     *     used books  10
     *     cheap books  23
     *     books for kindle  55
     *     children's books  101
     * </ul>
     * 
     * @param filename  The name of the output file.
     */
    def writeNodeIDMapping(filename:String){
        val file = FileIO.getBufferedLatinWriter(filename)
        nodeLabelToIDMap.foreach{case(n,nid) => 
            file.write(n)
            file.write("\t")
            file.write(nid.toString)
            file.write("\n")
        }
        file.close
    }

    /**
     * Clears the adjacency matrix. Useful if you want to free some memory.
     */
    def discardMatrix() = adjacencyMatrix.clear
}

/**
 * Companion for the ProcessGraph class.
 */
object ProcessGraph {

    val Node1Col = 0
    val Node2Col = 1
    val WeightCol = 2

    val NodeIDMappingFilename = "node-id-map.tsv.gz"
    val SparseAdjacencyMatrixFilename = "sparse-adj-matrix.tsv.gz"

    val Usage = """
    |Usage: ProcessGraph <node edge file> <out dir> [<node count estimate>]
    |
    | Produces three files with tab-delimited columns in <out dir>:
    |
    |   node-id-map.tsv.gz
    |   sparse-adj-matrix.tsv.gz
    |
    |""".stripMargin

    /**
     * A command line interface for creating a sparse adjacency matrix and a
     * node-to-id mapping. See Usage for details about how to call.
     * 
     * @param args  Command line params. Should consist of a node edge
     *              file and an output directory at a minimum.
     */
    def main(args:Array[String]){
        if( args.size < 2 || !ScalaFileIO.fileExists(args(0)))
            CommandLineIO.exitNice(Usage)

        val nodeFile = args(0)
        val outDir = args(1)
        val nodeCountEst = if( args.size > 2 ) args(2).toInt else 0

        if( !ScalaFileIO.fileExists(outDir) )
            new File(outDir).mkdirs()

        val pqp = new ProcessGraph(nodeFile, nodeCountEst)
        pqp.generateSparseAdjacencyMatrix()
        pqp.writeMatrix(outDir + File.separator + SparseAdjacencyMatrixFilename)
        pqp.writeNodeIDMapping(outDir + File.separator + NodeIDMappingFilename)
    }

}