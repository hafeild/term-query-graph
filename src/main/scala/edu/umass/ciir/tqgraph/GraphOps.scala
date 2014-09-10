// File:    GraphOps.scala
// Author:  Henry Feild
// Date:    13-Aug-2014
// Copyright 2014 Henry Feild
// License: Revised BSD -- see LICENSE in the root directory.

package edu.umass.ciir.tqgraph

import edu.umass.ciir.tqgraph.util.{
    ScalaFileIO,FileParser,FileIO,CommandLineIO,MathOps}
import scala.collection.mutable.{ArrayBuffer,HashMap,PriorityQueue}
import java.io.File

/**
 * Provides functionality for normalized random walks over graphs. The 
 * parameters are all optional. Don't forget to use either the setInputFiles() 
 * or setInputDir() methods. You can also use the apply methods. For example,
 * to use all the defaults and then supply a directory for the input files, do:
 *
 * val GraphOps = new GraphOps()(graphDir)
 *
 * Or to specify your own value for each of the constructor parameters:
 *
 * val GraphOps = new GraphOps(termCache, parallel, splitCount, alpha, 
 *           convergenceDistance, k)(graphDir)
 *
 * @param normalize            Default: false. If true, then the produced scores
 *                             will be normalized using the method described in
 *                             Boldi et al. (CIKM'08).
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
class GraphOps(var normalize:Boolean=false,
             val parallel:Boolean=false,
             val splitCount:Int=GraphOps.SplitCount,
             val alpha:Double=GraphOps.Alpha,
             val convergenceDistance:Double=GraphOps.ConvergenceDistance,
             var k:Long=GraphOps.K) {
    import GraphOps._
    type SparseFunction = (String, HashMap[Int,Double])=>Unit

    var rwr:RandomWalker = null
    var nidMap:HashMap[Int,String] = null
    var nLabelMap:HashMap[String,Int] = HashMap[String,Int]()
    var matrixRead = false

    var nodeIDMappingFilename=""
    var sparseAdjacencyMatrixFilename=""

    if( k < 0 ) k = Long.MaxValue

    /**
     * An alias for setInputFiles that also returns this instance.
     *
     * @param nodeIDMappingFilename  A file containing a mapping from query text
     *                               the query ids. Should be in the format:
     *                               <node-label>,<id>
     * @param sparseAdjacencyMatrixFilename  
     *                               A file containing the sparse adjacency
     *                               matrix; rows should correspond to 
     *                               source nodes. Each row should be in the 
     *                               form:
     *       <src-nid>,<target-nid1>,<prob1>,<target-nid2>,<prob2>,...
     * @return This instance.
     */
    def apply(nodeIDMappingFilename:String,
        sparseAdjacencyMatrixFilename:String):GraphOps = {
        setInputFiles(nodeIDMappingFilename, 
            sparseAdjacencyMatrixFilename)
        this
    }

    /**
     * Sets the term files.
     *
     * @param nodeIDMappingFilename  A file containing a mapping from query text
     *                               the query ids. Should be in the format:
     *                               <node-label>,<id>
     * @param sparseAdjacencyMatrixFilename  
     *                               A file containing the sparse adjacency
     *                               matrix; rows should correspond to 
     *                               source nodes. Each row should be in the 
     *                               form:
     *       <src-nid>,<target-nid1>,<prob1>,<target-nid2>,<prob2>,...
     */
    def setInputFiles(nodeIDMappingFilename:String,
        sparseAdjacencyMatrixFilename:String) {
        this.nodeIDMappingFilename = nodeIDMappingFilename
        this.sparseAdjacencyMatrixFilename = sparseAdjacencyMatrixFilename
    }

    /**
     * An alias for setInputDir that also returns the current instance.
     *
     * @param dir      The directory containing the term postings, rewrite
     *                 matrix, and query id mapping file.
     * @return This instance.
     */
    def apply(dir:String):GraphOps = {
        setInputDir(dir)
        this
    }

    /**
     * Given a directory, assumes the three input files are named as in 
     * ProcessGraph and class setInputFiles().
     * 
     * @param dir      The directory containing the term postings, rewrite
     *                 matrix, and query id mapping file.
     */
    def setInputDir(dir:String) {
        setInputFiles( 
            dir + File.separator + ProcessGraph.NodeIDMappingFilename,
            dir + File.separator + ProcessGraph.SparseAdjacencyMatrixFilename)
    }

    /**
     * Reads in the sparse adjacency matrix.
     */
    def readAdjacencyMatrix() {
        Console.err.print("Loading adjacency matrix...")
        rwr = parseAdjacencyMatrix(sparseAdjacencyMatrixFilename, parallel, 
            splitCount, convergenceDistance, alpha) 
        Console.err.println("done!")
    }

    /**
     * Reads in the query-to-id map.
     */
    def readNodeIDMap() {
        nidMap = parseNodeIDMapping(nodeIDMappingFilename)
        nidMap.foreach{case(nid,label) => nLabelMap(label) = nid}
    }

    /**
     * Looks up the node label associated with the given Pair. This is then
     * passed to the specified function along with the score.
     * 
     * @param pair     The query id, score pair to decode.
     * @param f        The function to which the decoded query text and score
     *                 will be passed.
     */
    def decodePairAndApply(pair:Pair, f:(String,Double)=>Unit) =
        f(nidMap(pair.nodeID), pair.score)

    /**
     * For a given node label, this will perform a random walk over the entire
     * graph with given initialization points (the e vector). The produced 
     * random walk results
     * will be output as a sparse vector in the specified output file in the
     * following format:
     * <ul>
     *   <term>,<nid1>,<score1>,<nid2>,<score2>,...
     * </ul>
     *
     * @param label       A label for this run (for cli output).
     * @param initializationVector A map of node labels to initialization 
     *                    values. This should be sparse; there's no need to 
     *                    include entries for nodes with a weight of 0. This
     *                    will be normalized so that all weights sum to 1. Note:
     *                    labels that have no corresponding id (i.e., are not a
     *                    part of the graph) are ignored.
     * @param k           The maximum number of results to return.
     * @param alpha       The restart frequency for a random walk.
     */
    def computeRandomWalkVector(label:String, 
            initializationVector:HashMap[String,Double], 
            k:Long=this.k, alpha:Double=this.alpha):Vector = {

        val results = PriorityQueue[Pair]()

        if(!matrixRead){
            readAdjacencyMatrix()
            matrixRead = true
        }

        // Add the initialization vector. The labels need to be converted to
        // their node ids.
        rwr.resetE()
        initializationVector.foreach{case(nodeLabel,weight) => 
            if(nLabelMap.contains(nodeLabel))
                rwr.addE(nLabelMap(nodeLabel), weight)
        }

        // Perform the random walk and assemble the results.
        Console.err.print("\tRandom walk for "+ label +", ")
        val runInfo = rwr.run()

        rwr.p.foreach{case(nid,value) =>
            if( value.value > 0 ){
                // results += new Pair(nid, value.value)
                results += new Pair(nid, math.log(value.value))
                // Remove the result with the smallest value if the queue
                // is too big (recall: pairs are ordered non-ascending, so
                // the max is actually the pair with the smallest value).
                if( results.size > k )
                    results.dequeue()
            }
        }

        val vec = new Vector(label, results)
        vec.isFirst = false
        vec
    }
}


object GraphOps {
    val Alpha = 0.9
    val ConvergenceDistance = 0.005
    val SplitCount = 4
    val MinCommandLineArgs = 2
    val K = -1L

    val Usage = """
    |Usage: GraphOps  <initialization vector file> <graph dir> [options]
    |
    |   Runs a random walk with restart for each initialization vector in
    |   <initialization vector file> using the graph stored in <graph dir>.
    |
    |   <initialization vector file> 
    |       Should have the format: 
    |           <label>\t<node1>\t<weight1>\t<node2>\t<weight2>...
    |       That is, columns are tab delimited and each line should start with a
    |       label followed by node-weight pairs. This is a sparse 
    |       representation, so nodes not present will have a weight of 0. 
    |       Weights will be normalized to sum to 1. Nodes should be represented 
    |       by their labels, not the internal ids located in the 
    |       <graph dir>/node-id-map.tsv.gz file. Nodes that are not found in the
    |       graph will be ignored.
    |
    |   <graph dir>
    |       Should contain two files:
    |           node-id-map.tsv.gz -- A map of node labels to ids.
    |           sparse-adj-matrix.tsv.gz -- The graph.
    |
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
    |   --normalize
    |       If present, the recommendations will be normalized.
    |
    |""".stripMargin

    /**
     * Reads and parses a sparse rewrite matrix from file. The file should have
     * the format:
     *     <src-nid>,<target-nid1>,<prob1>,<target-nid2>,<prob2>,...
     *
     * It will be read into a HashMap[Int,HashMap[Int,Double]].
     * 
     * @param adjacencyMatrixFilename  The name of the rewrite matrix file.
     * @param parallel         Whether to parallelize the random walks 
     *                         (default: false).
     * @param splitCount       If <code>parallel==true</code>, the random walk 
     *                         processing will be split into this many chunks.
     * @param convergenceDistance  The maximum distance between two iterations
     *                         of a random walk that count as convergence.

     * @return The rewrite matrix.
     */
    def parseAdjacencyMatrix(adjacencyMatrixFilename:String, 
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
            val n1id = cols(0).toInt
            val addTransition = rwr.addNodeTransitionsFnc(n1id)
            var i = 1
            while( i < cols.size-1 ){
                addTransition(cols(i).toInt, cols(i+1).toDouble)
                i += 2
            }
            x
        }

        // Read the file.
        FileParser.parseLatinFile(adjacencyMatrixFilename, true, parseLine)

        rwr
    }

    /**
     * Parses a query id mapping file, producing a map of query ids to
     * their corresponding query text.
     * 
     * @param filename     The name of the file to parse.
     * @return A map of query ids to their corresponding query text.
     */
    def parseNodeIDMapping(filename:String):HashMap[Int,String] = {
        def parseNodeIDMappingEntry(line:String, map:HashMap[Int,String]):
                HashMap[Int,String] = {
            val label = line.substring(0,line.lastIndexOf("\t"))
            val nid = line.substring(line.lastIndexOf("\t")+1).toInt

            map(nid) = label
            map
        }

        FileParser.parseLatinFile(
            filename, HashMap[Int,String](), parseNodeIDMappingEntry)
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
     * Process command line options.
     * 
     * @param args  The command line parameters.
     */
    def main(args:Array[String]) {
        if( args.size < MinCommandLineArgs )
            CommandLineIO.exitNice(Usage)

        val initializationVectorsFile = args(0)
        val graphDir = args(1)

        // Defaults for optional parameters.
        var parallel = false
        var alpha = Alpha
        var splitCount = SplitCount
        var convergenceDistance = ConvergenceDistance
        var k = K
        var normalize = false

        for( i <- MinCommandLineArgs until args.length){
            if( args(i).startsWith("--alpha=") )
                alpha = args(i).replaceAll("--alpha=", "").toDouble
            else if( args(i) == "--parallel" )
                parallel = true
            else if( args(i) == "--normalize" )
                normalize = true
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
        Console.err.println("\tnormalize: "+ normalize)

        if( k < 0 )
            k = Long.MaxValue

        // Read in the graph and associated files.
        val graphOps = new GraphOps(normalize, parallel, splitCount, alpha, 
            convergenceDistance, k)(graphDir)
        graphOps.readNodeIDMap()

        // Ready the recommender class, which will take care of generating
        // the recommendations and performing normalization.
        val recommender = new NodeRecommender(graphOps)

        // Process each initialization vector in the query file.        
        def parseInitilizationVectorList(line:String, foo:Boolean):Boolean = {
            val cols = line.trim.split("\\t")
            // Read the label (first column).
            val label = cols(0)
            val initializationVector = HashMap[String,Double]()
            var curCol = 1

            // Read each of the (node label, weight) pairs.
            while(curCol < cols.size-1) {
                initializationVector(cols(curCol)) = cols(curCol+1).toDouble
                curCol += 2
            }
            
            // Generate the recommendation vector.
            val recVector = recommender.getRecommendationVector(label, 
                initializationVector)

            // Output the recommendations.
            Console.err.println("Processing ["+ label +"]:")
            print(label)
            recVector.pairs.map{case(nid,score) =>
                new Pair(nid, score)}.toList.sorted.take(
                    math.min(k, recVector.pairs.size.toLong).toInt).foreach( 
                    graphOps.decodePairAndApply( _, (rec:String,score:Double)=>
                        print("\t"+ rec +"\t"+ score)
                    )
                )
            println()
            foo
        }

        // Parse each line of the initialization vector file.
        FileParser.parseLatinFile(initializationVectorsFile, false, 
            parseInitilizationVectorList)
    }

    /**
     * Encapsulates a node id, score pair. A list of these can be sorted in
     * non-ascending order of score.
     * 
     * @param nodeID     The id of the query.
     * @param score       The score of the query.
     */
    class Pair(val nodeID:Int, val score:Double) extends Ordered[Pair] {
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
        override def toString():String = nodeID +"\t"+ score
    }

    /**
     * Holds a label and associated recommendation vector. This can be updated
     * with random walk vectors using the mult method.
     * 
     * @param label   A label for the vector.
     * @param pqPairs Optional. A priority queue of values. Defaults to null.
     */
    class Vector(val label:String, var pqPairs:PriorityQueue[Pair]=null) {
        val pairs = HashMap[Int,Double]()
        var isFirst = true

        def this(other:Vector) = {
            this(other.label)
            pairs ++= other.pairs
        }

        if( null != pqPairs ) {
            pqPairs.foreach(p => pairs(p.nodeID) = p.score )
            pqPairs = null
        }

        def toLogSpace() {
             pairs.foreach{case (nid,score) => 
                if( score > 0 )
                    pairs(nid) = math.log(score)
                else
                    pairs.remove(nid)
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
                other.pairs.foreach{case(nid, score) => pairs(nid) = score}
                isFirst = false
            } else {
                pairs.foreach{case (nid,score) =>
                    if(other.pairs.contains(nid)) {
                        val newScore = score * other.pairs(nid) * weight
                        pairs(nid) = newScore

                        if( java.lang.Double.isInfinite(newScore) )
                            Console.err.println("Infinity! nid="+ nid +
                                "; this.score="+score+
                                "; other.score="+other.pairs(nid)+
                                "; new score="+ newScore )
                    }
                    else
                        pairs.remove(nid)
                }
            }
        }

        def multLogSpace(other:Vector, weight:Double=1.0){
            if( isFirst ){
                other.pairs.foreach{case(nid, score) => pairs(nid) = score}
                isFirst = false
            } else {
                pairs.foreach{case (nid,score) =>
                    if(other.pairs.contains(nid)) {
                        val newScore = score + other.pairs(nid)+math.log(weight)
                        pairs(nid) = newScore

                        if( java.lang.Double.isInfinite(newScore) )
                            Console.err.println("Infinity! nid="+ nid +
                                "; this.score="+score+
                                "; other.score="+other.pairs(nid)+
                                "; new score="+ newScore )
                    }
                    else
                        pairs.remove(nid)
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
                other.pairs.foreach{case(nid, score)=>pairs(nid) = weight*score}
                isFirst = false
            } else {
                pairs.keys.toSet[Int].union(other.pairs.keys.toSet[Int]).
                    foreach{nid =>
                        pairs(nid) =  pairs.getOrElse(nid, 0.0) +
                            weight * other.pairs.getOrElse(nid, 0.0)
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
                other.pairs.foreach{case(nid, score)=> 
                    pairs(nid) = math.log(weight) + score
                }
                isFirst = false
            } else {
                pairs.keys.toSet[Int].union(other.pairs.keys.toSet[Int]).
                    foreach{nid =>
                        val scores = List(pairs.getOrElse(nid, 
                                Double.NegativeInfinity),
                            math.log(weight) + 
                            other.pairs.getOrElse(nid, Double.NegativeInfinity))

                        pairs(nid) = MathOps.logSumExp(scores)
                }
            }
        }        

        /**
         * Prints this vector.
         * @param delim     The delimiter to use. Default: "\t"
         */
        def print(delim:String="\t") = {
            Console.print( label )
            pairs.foreach{case(nid,score) => 
                Console.print(delim+nid+delim+score)} 
            Console.println()
        }
    }

    /**
     * Class for extracting recommendations.
     *
     * @param graphOps  An instance of GraphOps.
     */
    class NodeRecommender( graphOps:GraphOps ){
        import NodeRecommender._

        var uniformRWVector:Vector = null
         
        if( graphOps.normalize )
            mkUniformVector()

        /**
         * Converts the uniform vector to it's normalized form, i.e., take
         * the square root of each entry. This is done in log space.
         */
        def mkUniformVector() {
            val uniformInitVector = HashMap[String,Double]()
            graphOps.nLabelMap.keys.foreach{label => uniformInitVector(label)=1}

            uniformRWVector = graphOps.computeRandomWalkVector("UNIFORM",
                uniformInitVector, k=Long.MaxValue)

            uniformRWVector.pairs.foreach{case(nid,score) =>
                // Log space.
                val newScore = -0.5  * score
                uniformRWVector.pairs(nid) = newScore

                if( java.lang.Double.isInfinite(newScore) )
                    Console.err.println("Infinity! nid="+ nid +
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
         * Produces a recommendation vector for the given initialization vector.
         * 
         * @param label      A label for the given initialization vector.
         * @param initVector A map of node labels to their initialization value.
         *                   This should be a sparse representation. Missing
         *                   node labels will be assumed to have a starting
         *                   weight of 0. Node labels that are not part of 
         *                   the graph will be ignored.
         * @return The recommendation vector for the given initialization 
         *         vector.
         */
        def getRecommendationVector(label:String, 
                initVector:HashMap[String,Double]):Vector = {
            val rwVector = graphOps.computeRandomWalkVector(label, initVector)
            if(graphOps.normalize)
                normalizeVector(rwVector)

            rwVector
        }
    }

    object NodeRecommender {
        val UniformVector = "::UNIFORM::"
    }
}