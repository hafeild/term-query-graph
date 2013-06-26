// File:    ProcessQueryPairs.scala
// Author:  Henry Feild
// Date:    20-Nov-2012
// File:    ProcessQueryPairs.scala
// Author:  Henry Feild
// Date:    20-Dec-2012
// Copyright 2013 Henry Feild
// License: Revised BSD -- see LICENSE in the root directory.

package edu.umass.ciir.tqgraph

import edu.umass.ciir.tqgraph.util.{ScalaFileIO,FileParser,FileIO,CommandLineIO}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import java.io.File

/**
 * Takes a set of query reformulation counts and produces three things:
 * <ul>
 *     <li>query-id --> query-text mapping
 *     <li>term --> query-ids postings
 *     <li>sparse rewrite adjacency matrix; rows sum to 1
 * </ul>
 * Currently these are all written to text files.
 *
 * @author hfeild
 */
class ProcessQueryPairs(rewriteFile:String, queryCountEst:Int=0) {
    import ProcessQueryPairs._

    val queryToIDMap = new HashMap[String,Int]()
    val rewriteMatrix = new HashMap[Int,HashMap[Int,Double]]()
    val termPostings = new HashMap[String, ArrayBuffer[Int]]()

    // If the caller gave a query count estimate, we can use that as a hint
    // to our hash maps.
    if( queryCountEst > 0 ) {
        queryToIDMap.sizeHint(queryCountEst)
        rewriteMatrix.sizeHint(queryCountEst)
        termPostings.sizeHint(queryCountEst)
    }

    /**
     * Adds the given query string to the query id map if it's not already
     * in there. It will be assigned the next available id.
     * 
     * @param query The query to add.
     */
    def addToMapIfNeeded(query:String) {
        if( !queryToIDMap.contains(query) )
            queryToIDMap(query) = queryToIDMap.size
    }

    /**
     * Adds a term to the term postings list if it's not already.
     * 
     * @param term The term to add.
     */
    def addToTermPostingsIfNeeded(term:String) {
        if( !termPostings.contains(term) )
            termPostings(term) = new ArrayBuffer[Int]()
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
        val query1 = cols(Query1Col)
        val query2 = cols(Query2Col)
        val count = cols(FrequencyCol).toDouble

        if( query1 == query2 )
            return seenHeader
            
        addToMapIfNeeded(query1)
        addToMapIfNeeded(query2)

        if( !rewriteMatrix.contains(queryToIDMap(query1)) )
            rewriteMatrix(queryToIDMap(query1)) = new HashMap[Int,Double]()

        rewriteMatrix(queryToIDMap(query1))(queryToIDMap(query2)) = count

        seenHeader
    }

    /**
     * Normalizes the rows of the rewrite matrix so that the outgoing edges
     * of each query sums to 1.
     */
    def normalizeRows(){
        rewriteMatrix.values.foreach{queries =>
            val sum = queries.values.sum.toDouble
            queries.foreach{case(q,v) =>
                queries(q) = v / sum 
            }
        }
    }

    /**
     * Reads in and parses the rewrite file, creating a normalized, sparse 
     * re-write matrix,
     */
    def generateSparseRewriteMatrix() {
        FileParser.parseLatinFile(rewriteFile, false, parseInputLine)
        normalizeRows()
    }

    /**
     * Generates a term postings list.
     */
    def generateTermPostings(){
        queryToIDMap.foreach{case(q,id) =>
            val terms = q.toLowerCase.split("\\W+")
            terms.toSet.foreach{t:String => 
                addToTermPostingsIfNeeded(t)
                termPostings(t) += id
            }
        }
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
        rewriteMatrix.foreach{case(qid, queries) =>
            file.write(qid.toString)
            queries.foreach{case(qid2,count) =>
                file.write("\t")
                file.write(qid2.toString)
                file.write("\t")
                file.write(count.toString)
            }
            file.write("\n")
        }
        file.close()
    }

    /**
     * Writes the term postings map to file. The output format is:
     * [term]\t[query1 id]\t[query2 id],...
     * where queryi contains [term]. E.g.
     * <ul>
     *     books  1  10  23  55  101
     * </ul>
     * Where the query mapping might be: 1 -> "free books", 10 -> "used books", 
     * 23 -> "cheap books", 55 -> "books for kindle", and 101 -> 
     * "children s books".
     * 
     * @param filename  The name of the output file.
     */
    def writeTermPostings(filename:String){
        val file = FileIO.getBufferedLatinWriter(filename)
        termPostings.foreach{case(term,qids) =>
            file.write(term)
            qids.foreach{qid => file.write("\t"); file.write(qid.toString)}
            file.write("\n")
        }
        // Write the uniform vector.
        file.write("::UNIFORM::")
        queryToIDMap.values.foreach(id => file.write("\t"+ id))
        file.write("\n")
        file.close
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
    def writeQueryIDMapping(filename:String){
        val file = FileIO.getBufferedLatinWriter(filename)
        queryToIDMap.foreach{case(q,qid) => 
            file.write(q)
            file.write("\t")
            file.write(qid.toString)
            file.write("\n")
        }
        file.close
    }

    /**
     * Clears the re-write matrix. Useful if you want to free some memory.
     */
    def discardRewriteMatrix() = rewriteMatrix.clear
}

/**
 * Companion for the ProcessQueryPairs class.
 */
object ProcessQueryPairs {

    val Query1Col = 0
    val Query2Col = 1
    val FrequencyCol = 2

    val TermPostingsFilename = "term-postings.tsv.gz"
    val QueryIDMappingFilename = "query-id-map.tsv.gz"
    val QueryReformulationMatrixFilename = "query-rewrite-matrix.tsv.gz"

    val Usage = """
    |Usage: ProcessQueryPairs <query pair file> <out dir> [<query count est>]
    |
    | Produces three files with tab-delimited columns in <out dir>:
    |
    |   term-postings.tsv.gz
    |   query-id-map.tsv.gz
    |   query-rewrite-matrix.tsv.gz
    |
    |""".stripMargin

    /**
     * A command line interface for creating a sparse re-write matrix, a
     * query-to-id mapping, and a term postings list. See Usage for details 
     * about how to call.
     * 
     * @param args  Command line params. Should consist of a query reformulation
     *              file and an output directory at a minimum.
     */
    def main(args:Array[String]){
        if( args.size < 2 || !ScalaFileIO.fileExists(args(0)))
            CommandLineIO.exitNice(Usage)

        val queryPairFile = args(0)
        val outDir = args(1)
        val queryCountEst = if( args.size > 2 ) args(2).toInt else 0

        if( !ScalaFileIO.fileExists(outDir) )
            new File(outDir).mkdirs()

        val pqp = new ProcessQueryPairs(queryPairFile, queryCountEst)
        pqp.generateSparseRewriteMatrix()
        pqp.writeMatrix(outDir+"/"+QueryReformulationMatrixFilename)
        pqp.writeQueryIDMapping(outDir+"/"+QueryIDMappingFilename)

        // Free up some memory...
        pqp.discardRewriteMatrix()

        pqp.generateTermPostings()
        pqp.writeTermPostings(outDir+"/"+TermPostingsFilename)
    }

}