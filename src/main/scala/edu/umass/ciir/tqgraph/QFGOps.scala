// File:    QFGOps.scala
// Author:  Henry Feild
// Date:    21-Nov-2012
// Copyright 2013 Henry Feild
// License: Revised BSD -- see LICENSE in the root directory.

package edu.umass.ciir.tqgraph

import edu.umass.ciir.tqgraph.util.{
    ScalaFileIO,FileParser,FileIO,CommandLineIO,MathOps}
import scala.collection.mutable.{ArrayBuffer,HashMap,PriorityQueue}
import java.io.File

/**
 * Provides functionality involving Query Flow Graphs (QFGs) and Term-Query
 * Graphs (TQGraphs). These are all optional. Don't forget to use either the
 * setInputFiles() or setInputDir() methods. You can also use the apply methods.
 * For example, to use all the defaults and then supply a directory for the
 * input files, do:
 *
 * val qfgOps = new QFGOps()(qfgDir)
 *
 * Or to specify your own value for each of the constructor parameters:
 *
 * val qfgOps = new QFGOps(termCache, parallel, splitCount, alpha, 
 *           convergenceDistance, k)(qfgDir)
 *
 * @param termCache            The directory of the term cache files.
 * @param parallel             Whether to parallelize the random walks 
 *                             (default: false).
 * @param splitCount           If <code>parallel==true</code>, the random  
 *                             walk processing will be split into this many
 *                             chunks.
 * @param convergenceDistance  The maximum distance between two iterations
 *                             of a random walk that count as convergence.
 * @param k                    The number of top nodes from a term's random
 *                             walk to emit.
 * @author hfeild
 */
class QFGOps(val termRandomWalkCache:String="",
             var parallel:Boolean=false,
             var splitCount:Int=QFGOps.SplitCount,
             var alpha:Double=QFGOps.Alpha,
             var convergenceDistance:Double=QFGOps.ConvergenceDistance,
             var k:Long=QFGOps.K) {
    import QFGOps._
    type SparseFunction = (String, HashMap[Int,Double])=>Unit

    var rwr:RandomWalker = null
    var qidMap:HashMap[Int,String] = null
    var qfgRead = false

    var termPostingsFilename=""
    var queryIDMappingFilename=""
    var queryReformulationMatrixFilename=""

    /**
     * An alias for setInputFiles that also returns this instance.
     *
     * @param termPostingsFilename   The term postings file. Each line should be
     *                               in the format: <term>,<qid1>,<qid2>...
     * @param queryIDMappingFilename A file containing a mapping from query text
     *                               the query ids. Should be in the format:
     *                               <query-text>,<id>
     * @param queryReformulationMatrixFilename  
     *                               A file containing the sparse matrix of 
     *                               query rewrites; rows should correspond to 
     *                               source queries. Each row should be in the 
     *                               form:
     *       <src-qid>,<target-qid1>,<prob1>,<target-qid2>,<prob2>,...
     * @return This instance.
     */
    def apply(termPostingsFilename:String,queryIDMappingFilename:String,
        queryReformulationMatrixFilename:String):QFGOps = {
        setInputFiles(termPostingsFilename, queryIDMappingFilename, 
            queryReformulationMatrixFilename)
        this
    }

    /**
     * Sets the term files.
     *
     * @param termPostingsFilename   The term postings file. Each line should be
     *                               in the format: <term>,<qid1>,<qid2>...
     * @param queryIDMappingFilename A file containing a mapping from query text
     *                               the query ids. Should be in the format:
     *                               <query-text>,<id>
     * @param queryReformulationMatrixFilename  
     *                               A file containing the sparse matrix of 
     *                               query rewrites; rows should correspond to 
     *                               source queries. Each row should be in the 
     *                               form:
     *       <src-qid>,<target-qid1>,<prob1>,<target-qid2>,<prob2>,...
     */
    def setInputFiles(termPostingsFilename:String,queryIDMappingFilename:String,
        queryReformulationMatrixFilename:String) {
        this.termPostingsFilename = termPostingsFilename
        this.queryIDMappingFilename = queryIDMappingFilename
        this.queryReformulationMatrixFilename = queryReformulationMatrixFilename
    }

    /**
     * An alias for setInputDir that also returns the current instance.
     *
     * @param dir      The directory containing the term postings, rewrite
     *                 matrix, and query id mapping file.
     * @return This instance.
     */
    def apply(dir:String):QFGOps = {
        setInputDir(dir)
        this
    }

    /**
     * Given a directory, assumes the three input files are named as in 
     * ProcessQueryPairs and class setInputFiles().
     * 
     * @param dir      The directory containing the term postings, rewrite
     *                 matrix, and query id mapping file.
     */
    def setInputDir(dir:String) {
        setInputFiles( 
            dir + File.separator + ProcessQueryPairs.TermPostingsFilename,
            dir + File.separator + ProcessQueryPairs.QueryIDMappingFilename ,
            dir + File.separator + ProcessQueryPairs.
                QueryReformulationMatrixFilename )
    }



    if( k < 0 ) k = Long.MaxValue

    // Create the term cache directory if it doesn't exist.
    if( !new File(termRandomWalkCache).exists() )
        new File(termRandomWalkCache).mkdirs()

    /**
     * Reads in the sparse re-write matrix.
     */
    def readRewriteMatrix() {
        Console.err.print("Loading transition matrix...")
        rwr = parseRewriteMatrix(queryReformulationMatrixFilename, parallel, 
            splitCount, convergenceDistance) 
        Console.err.println("done!")
    }

    /**
     * Reads in the query-to-id map.
     */
    def readQueryIDMap() {
        qidMap = parseQueryIDMapping(queryIDMappingFilename)
    }


    /**
     * Parses the given line from the term postings file and applies the given
     * function to the postings. The function should expect a term and a
     * HashMap[Int,Int] structure.
     *
     * @param line     A term posting. Should be in the format:
     *                 <term>,<qid1>,<qid2>...
     * @param info     Should contain a list of white listed terms and a 
     *                 function that operates on the posting translated into
     *                 a hash.
     * @return info, untouched.
     */
    def parseTermPostingAndApply(line:String, info:TermPostingInfo):
            TermPostingInfo = {

        var cols = line.split("\t")

        // Check that this entry is one that is wanted.
        if( info.terms.contains(cols(0)) ){
            // Build the map.
            for( i <- 1 until cols.size )
                rwr.addE(cols(i).toInt, 1.0)

            // Invoke the given function.
            info.f(cols(0))
            rwr.resetE()
        }
        info
    }


    /**
     * Looks up the query text associated with the given Pair. This is then
     * passed to the specified function along with the score.
     * 
     * @param pair     The query id, score pair to decode.
     * @param f        The function to which the decoded query text and score
     *                 will be passed.
     */
    def decodePairAndApply(pair:Pair, f:(String,Double)=>Unit) =
        f(qidMap(pair.queryID), pair.score)

    /**
     * For each term in a list of terms, this will perform a random walk 
     * over the entire QFG with initialization points (the e vector) at the 
     * queries in the term's posting list. The produced random walk results
     * will be output as a sparse vector in the specified output file in the
     * following format:
     * <ul>
     *   <term>,<qid1>,<score1>,<qid2>,<score2>,...
     * </ul>
     * This is faster than computeRandomWalkVector because here, the term
     * postings list is only read once, whereas computeRandomWalkVector has
     * to process the entire list per request.
     *
     * @param terms       A set of terms over which to compute random walks.
     * @param k           The maximum number of results to return.
     * @param alpha       The restart frequency for a random walk.
     */
    def computeTermRandomWalkVectors(terms:Set[String], 
            k:Long=this.k, alpha:Double=this.alpha) {

        val results = PriorityQueue[Pair]()

        if(!qfgRead){
            readRewriteMatrix()
            qfgRead = true
        }

        // Called once the RWR object has been updated for the given term.
        def randomWalkAndSave(term:String) {
            results.clear
            Console.err.print("\t["+ term +"]:")
            val runInfo = rwr.run()

            val cacheFilename = genCacheFilename(term)

            // Check if it's in the cache before performing the random walk.
            // If so, there's nothing more to do for this term.
            if( existsInCache(term) ) return

            rwr.p.foreach{case(qid,value) =>
                if( value.value > 0 ){
                    results += new Pair(qid, math.log(value.value))
                    // Remove the result with the smallest value if the queue
                    // is too big (recall: pairs are ordered non-ascending, so
                    // the max is actually the pair with the smallest value).
                    if( results.size > k )
                        results.dequeue()
                }
            }

            // Write to the cache.
            val out = FileIO.getBufferedLatinWriter(cacheFilename)
            out.write(term)
            results.foreach(p => {
                out.write("\t"+ p.queryID +"\t"+ p.score.toString)
            })
            out.close
        }

        // Parse!
        FileParser.parseLatinFile(termPostingsFilename, 
            new TermPostingInfo(terms, randomWalkAndSave),
            parseTermPostingAndApply )
    }

    /**
     * For each term in a list of terms, this will perform a random walk 
     * over the entire QFG with initialization points (the e vector) at the 
     * queries in the term's posting list. The produced random walk results
     * will be output as a sparse vector in the specified output file in the
     * following format:
     * <ul>
     *   <term>,<qid1>,<score1>,<qid2>,<score2>,...
     * </ul>
     * @param term        The term to compute a random walk for.
     * @param k           The maximum number of results to return.
     * @param alpha       The restart frequency for a random walk.
     */
    def computeTermRandomWalkVector(term:String, k:Long=this.k, 
            alpha:Double=this.alpha):Vector = {

        val results = PriorityQueue[Pair]()

        val cacheFilename = genCacheFilename(term)

        // Check if it's in the cache before performing the random walk.
        if( existsInCache(term) )
            return parseRandomWalkVectorEntry(
                ScalaFileIO.slurpFile(cacheFilename).trim)

        if(!qfgRead){
            readRewriteMatrix()
            qfgRead = true
        }

        // Called once the RWR object has been updated for the given term.
        def randomWalkAndSave(term:String) {
            Console.err.print("\tRandom walk for ["+ term +"], ")
            rwr.run()
            rwr.p.foreach{case(qid,value) =>
                if( value.value > 0 ){
                    results += new Pair(qid, math.log(value.value))
                    // Remove the result with the smallest value if the queue
                    // is too big (recall: pairs are ordered non-ascending, so
                    // the max is actually the pair with the smallest value).
                    if( results.size > k )
                        results.dequeue()
                }
            }
        }

        // Parse!
        FileParser.parseLatinFile(termPostingsFilename, 
            new TermPostingInfo(Set(term), randomWalkAndSave),
            parseTermPostingAndApply )

        // Write to the cache.
        val out = FileIO.getBufferedLatinWriter(cacheFilename)
        out.write(term)
        results.foreach(p => {
            out.write("\t"+ p.queryID +"\t"+ p.score.toString)
        })
        out.close

        val vec = new Vector(term, results)
        vec.isFirst = false
        vec
    }

    def existsInCache(term:String):Boolean = 
        new File(genCacheFilename(term)).exists()

    def genCacheFilename(term:String):String = 
        termRandomWalkCache + File.separator + term +".gz"
}


object QFGOps {
    val Alpha = 0.9
    val ConvergenceDistance = 0.005
    val SplitCount = 4
    val MinCommandLineArgs = 3
    val K = -1L

    val Usage = """
    |Usage: QFGOps  <query file> <rewrite matrix dir> <term cache dir> [options]
    |
    |   Divides each query into terms (after replacing punctuation with a space)
    |   and performs a random walk on each term. The random walk vectors for 
    |   each term are written to a file in <term cache dir>. The term vectors
    |   for the terms in a query are then combined to produce the resulting
    |   recommendations. Recommendations are output to stdout.
    |
    |Options:
    |
    |   --alpha=<alpha>
    |       The restart probability for the random walk. 
    |       Default: 0.9
    |
    |   --k=<num>
    |       The number of top recommendations to use per term and per query. 
    |       Consider setting this to 1,000 or 10,000 to improve performance.
    |       Set to -1 for all.
    |       Default: -1
    |
    |   --parallel
    |       If present, then the random walk for a term will be split up into
    |       <split-count> parts and the parts carried out in parallel.
    |       Default: false
    |
    |   --split-count=<num>
    |       The number of parts to split each random walk into when done in
    |       parallel. 
    |       Default: 4
    |
    |   --convergence-distance=<num>
    |       The maximum distance between two consecutive iterations of the 
    |       random walk algorithm that should be viewed as signifying 
    |       convergence. 
    |       Default: 0.005
    |
    |""".stripMargin

    /**
     * Reads and parses a sparse rewrite matrix from file. The file should have
     * the format:
     *     <src-qid>,<target-qid1>,<prob1>,<target-qid2>,<prob2>,...
     *
     * It will be read into a HashMap[Int,HashMap[Int,Double]].
     * 
     * @param rewriteFilename  The name of the rewrite matrix file.
     * @param parallel         Whether to parallelize the random walks 
     *                         (default: false).
     * @param splitCount       If <code>parallel==true</code>, the random walk 
     *                         processing will be split into this many chunks.
     * @param convergenceDistance  The maximum distance between two iterations
     *                         of a random walk that count as convergence.

     * @return The rewrite matrix.
     */
    def parseRewriteMatrix(rewriteFilename:String, 
            parallel:Boolean=false, splitCount:Int=SplitCount, 
            convergenceDistance:Double=ConvergenceDistance,
            alpha:Double=Alpha):RandomWalker = {

        // Holds the sparse matrix.
        val rwr:RandomWalker = 
            if(parallel)
                new RandomWalkParallelized(1-alpha, splitCount=splitCount,
                    convergenceDistance=convergenceDistance)
            else
                new RandomWalk(1-alpha, convergenceDistance=convergenceDistance)

        // Parses a single line of the rewrite matrix file.
        def parseLine(line:String, x:Boolean):Boolean = {
            val cols = line.split("\t")
            val q1id = cols(0).toInt
            val addTransition = rwr.addNodeTransitionsFnc(q1id)
            var i = 1
            while( i < cols.size ){
                addTransition(cols(i).toInt, cols(i+1).toDouble)
                i += 2
            }
            x
        }

        // Read the file.
        FileParser.parseLatinFile(rewriteFilename, true, parseLine)

        rwr
    }

    /**
     * Parses a query id mapping file, producing a map of query ids to
     * their corresponding query text.
     * 
     * @param filename     The name of the file to parse.
     * @return A map of query ids to their corresponding query text.
     */
    def parseQueryIDMapping(filename:String):HashMap[Int,String] = {
        def parseQueryIDMappingEntry(line:String, map:HashMap[Int,String]):
                HashMap[Int,String] = {
            val query = line.substring(0,line.lastIndexOf("\t"))
            val qid = line.substring(line.lastIndexOf("\t")+1).toInt

            map(qid) = query
            map
        }

        FileParser.parseLatinFile(
            filename, HashMap[Int,String](), parseQueryIDMappingEntry)
    }

    /**
     * Converts a line from a random walk vector file to a vector.
     *
     * @param line  The line.
     * @return A vector.
     */
    def parseRandomWalkVectorEntry(line:String):Vector = {
        val cols = line.split("\t")
        val vector = new Vector(cols(0))
        var i = 1
        while( i < cols.size) {
            if(cols(i+1) != "0.0")
                vector.pairs(cols(i).toInt) = cols(i+1).toDouble
            i += 2
        }
        vector.isFirst = false;
        vector
    }
    
    /**
     * Removes all punctuation and splits on whitespace.
     * 
     * @param query  The query to tokenize.
     * @return The tokens of the query.
     */
    def tokenize(query:String):Array[String] = clean(query).split("\\s")
    
    /**
     * Removes all punctuation and extra whitespace.
     * 
     * @param query The query to clean.
     * @return The cleaned query.
     */
    def clean(query:String):String = 
        query.toLowerCase.replaceAll("\\W+", " ").trim

    /**
     * Process command line options.
     * 
     * @param args  The command line parameters.
     */
    def main(args:Array[String]) {
        if( args.size < MinCommandLineArgs )
            CommandLineIO.exitNice(Usage)

        val queryFile = args(0)
        val qfgDir = args(1)
        val termCache = args(2)

        // Defaults for optional parameters.
        var parallel = false
        var alpha = Alpha
        var splitCount = SplitCount
        var convergenceDistance = ConvergenceDistance
        var k = K

        for( i <- MinCommandLineArgs until args.length){
            if( args(i).startsWith("--alpha=") )
                alpha = args(i).replaceAll("--alpha=", "").toDouble
            else if( args(i) == "--parallel" )
                parallel = true
            else if( args(i).startsWith("--split-count=") )
                splitCount = args(i).replaceAll("--split-count=", "").toInt
            else if( args(i).startsWith("--convergence-distance=") )
                convergenceDistance = args(i).replaceAll(
                    "--convergence-distance=","").toDouble
            else if( args(i).startsWith("--k=") )
                k = args(i).replaceAll("--k=", "").toLong
            else
                CommandLineIO.exitNice("Unrecognized argument: "+ args(i) +
                    "\n\n"+ Usage)
        }

        Console.err.println("Using:")
        Console.err.println("\tparaellel: "+ parallel)
        Console.err.println("\tsplit count: "+ splitCount)
        Console.err.println("\talpha: "+ alpha)
        Console.err.println("\tconvergence dist.: "+ convergenceDistance)
        Console.err.println("\tk: "+ k)

        if( k < 0 )
            k = Long.MaxValue

        val qfgOps = new QFGOps(termCache, parallel, splitCount, alpha, 
            convergenceDistance, k)(qfgDir)
        qfgOps.readQueryIDMap()
        val recommender = new QueryRecommender(termCache, qfgOps, k)

        // Process each query in the query file.        
        def parseQueryList(line:String, foo:Boolean):Boolean = {
            val query = line.trim
            val recVector = recommender.getRecommendationVector(query)
            Console.err.println("Processing ["+ query +"]:")
            print(query)
            recVector.pairs.map{case(qid,score) =>
                new Pair(qid, score)}.toList.sorted.take(
                    math.min(k, recVector.pairs.size.toLong).toInt).foreach( 
                    qfgOps.decodePairAndApply( _, (rec:String,score:Double)=>
                        print("\t"+ rec +"\t"+ score)
                    )
                )
            println()
            foo
        }

        FileParser.parseLatinFile(queryFile, false, parseQueryList)
    }

    /**
     * Helper class for parsing term postings (i.e., term -> queries containing
     * the term).
     * 
     * @param terms     A set of terms.
     * @param f         A function that can act on a term.
     */
    class TermPostingInfo(val terms:Set[String], val f:(String)=>Unit)

    /**
     * Encapsulates a query id, score pair. A list of these can be sorted in
     * non-ascending order of score.
     * 
     * @param queryID     The id of the query.
     * @param score       The score of the query.
     */
    class Pair(val queryID:Int, val score:Double) extends Ordered[Pair] {
        /**
         * For sorting in non-ascending order of score.
         * @param that     Another pair to compare with this one.
         * @return 0 if the scores are equal, -1 if this score is higher, 1 
         *  otherwise.
         */
        def compare(that:Pair):Int = {
            if(that.score - this.score < 0) -1 
            else if(that.score == this.score) 0 
            else 1
        }

        /**
         * Returns the query id, score pair as a comma separated string.
         * @return The query id, score pair as a comma separated string.
         */
        override def toString():String = queryID +"\t"+ score
    }

    /**
     * Holds a query string and it's recommendation vector. This can be updated
     * with random walk vectors using the mult method.
     * 
     * @param query   The query text.
     */
    class Vector(val query:String, var pqPairs:PriorityQueue[Pair]=null) {
        val pairs = HashMap[Int,Double]()
        var isFirst = true

        def this(other:Vector) = {
            this(other.query)
            pairs ++= other.pairs
        }

        if( null != pqPairs ) {
            pqPairs.foreach(p => pairs(p.queryID) = p.score )
            pqPairs = null
        }

        def toLogSpace() {
             pairs.foreach{case (qid,score) => 
                if( score > 0 )
                    pairs(qid) = math.log(score)
                else
                    pairs.remove(qid)
            }
        }

        /**
         * Multiplies the given vector (e.g., a random walk vector) with this
         * one in an element-wise fashion. Assumes a sparse vector, any elements
         * not in the intersection of the two vectors will be removed.
         * 
         * @param other   The vector to multiply with this one.
         */
        def mult(other:Vector, weight:Double=1.0){
            if( isFirst ){
                other.pairs.foreach{case(qid, score) => pairs(qid) = score}
                isFirst = false
            } else {
                pairs.foreach{case (qid,score) =>
                    if(other.pairs.contains(qid)) {
                        val newScore = score * other.pairs(qid) * weight
                        pairs(qid) = newScore

                        if( java.lang.Double.isInfinite(newScore) )
                            Console.err.println("Infinity! qid="+ qid +
                                "; this.score="+score+
                                "; other.score="+other.pairs(qid)+
                                "; new score="+ newScore )
                    }
                    else
                        pairs.remove(qid)
                }
            }
        }

        def multLogSpace(other:Vector, weight:Double=1.0){
            if( isFirst ){
                other.pairs.foreach{case(qid, score) => pairs(qid) = score}
                isFirst = false
            } else {
                pairs.foreach{case (qid,score) =>
                    if(other.pairs.contains(qid)) {
                        val newScore = score + other.pairs(qid)+math.log(weight)
                        pairs(qid) = newScore

                        if( java.lang.Double.isInfinite(newScore) )
                            Console.err.println("Infinity! qid="+ qid +
                                "; this.score="+score+
                                "; other.score="+other.pairs(qid)+
                                "; new score="+ newScore )
                    }
                    else
                        pairs.remove(qid)
                }
            }
        }

        /**
         * Adds the other vector to this one. After this operation has finished,
         * there will be one entry for each key in the union of the two vectors.
         * 
         * @param other     The other vector to add to this one.
         * @param weight    The weight to give the other vector's scores.
         */
        def add(other:Vector, weight:Double=1.0){
            if( isFirst ){
                other.pairs.foreach{case(qid, score)=>pairs(qid) = weight*score}
                isFirst = false
            } else {
                pairs.keys.toSet[Int].union(other.pairs.keys.toSet[Int]).
                    foreach{qid =>
                        pairs(qid) =  pairs.getOrElse(qid, 0.0) +
                            weight * other.pairs.getOrElse(qid, 0.0)
                }
            }
        }

        /**
         * Adds the other vector to this one, but assumes 'other' is in log
         * space. After this operation has finished, there will be one entry for
         * each key in the union of the two vectors.
         * 
         * @param other     The other vector to add to this one.
         * @param weight    The weight to give the other vector's scores. This 
         *                  is assumed to NOT be in log space.
         */
        def addLogs(other:Vector, weight:Double=1.0){
            if( isFirst ){
                other.pairs.foreach{case(qid, score)=> 
                    pairs(qid) = math.log(weight) + score
                }
                isFirst = false
            } else {
                pairs.keys.toSet[Int].union(other.pairs.keys.toSet[Int]).
                    foreach{qid =>
                        val scores = List(pairs.getOrElse(qid, 
                                Double.NegativeInfinity),
                            math.log(weight) + 
                            other.pairs.getOrElse(qid, Double.NegativeInfinity))

                        pairs(qid) = MathOps.logSumExp(scores)
                }
            }
        }        

        /**
         * Prints this vector.
         * @param delim     The delimiter to use. Default: "\t"
         */
        def print(delim:String="\t") = {
            Console.print( clean(query) )
            pairs.foreach{case(qid,score) => 
                Console.print(delim+qid+delim+score)} 
            Console.println()
        }
    }

    /**
     * Class for extracting recommendations.
     *
     * @param termRWVectorDir     The directory containing term random walk
     *                            vectors. Each term should have its own file
     *                            with the term as the filename.
     */
    class QueryRecommender( val termRWVectorDir:String, qfgOps:QFGOps, 
            k:Double=K ){
        import QueryRecommender._

        var uniformRWVector:Vector = null
         
        mkUniformVector()

        /**
         * Converts the uniform vector to it's normalized form, i.e., take
         * the square root of each entry. This is done in log space.
         */
        def mkUniformVector() {
            uniformRWVector = qfgOps.computeTermRandomWalkVector(
                UniformVector, k=Long.MaxValue)
            uniformRWVector.pairs.foreach{case(qid,score) =>
                // Log space.
                val newScore = -0.5  * score
                uniformRWVector.pairs(qid) = newScore

                if( java.lang.Double.isInfinite(newScore) )
                    Console.err.println("Infinity! qid="+ qid +
                        "; score="+score+"; new score="+ newScore )
            }
        }

        /**
         * Normalizes a random walk vector in place using the uniformRWVector.
         *
         * @param vector     The vector to normalize.
         */
        def normalizeVector(vector:Vector) = 
            vector.multLogSpace(uniformRWVector)

        /**
         * Extracts the random walk vector for the specified term, normalizes
         * it using the uniform random walk vector, and multiplies with the
         * the given vector.
         * 
         * @param queryVector     The vector to multiply the term vector with.
         * @param term            The term to apply.
         */
        def applyTermRecommendationVector(queryVector:Vector, term:String){
            val rwVector = qfgOps.computeTermRandomWalkVector(term)
            normalizeVector(rwVector)
            queryVector.multLogSpace(rwVector)
            rwVector.pairs.clear
        }

        /**
         * Produces a recommendation vector for the given query.
         * 
         * @param query     The query to compute recommendation for.
         * @return The recommendation vector for the given query.
         */
        def getRecommendationVector(query:String):Vector = {
            val vector = new Vector(query)

            // Tokenize the query and add the vector to each token's entry.
            tokenize(query).toSet[String].foreach{token =>
                applyTermRecommendationVector(vector, token)
            }
            vector
        }
    }

    object QueryRecommender {
        val UniformVector = "::UNIFORM::"
    }
}




