/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.mcmaster.sptestdriverxy;
 
import static ca.mcmaster.spcplexlibxy.Constants.*;
import static ca.mcmaster.spcplexlibxy.Parameters.*;
import ca.mcmaster.spcplexlibxy.datatypes.*;
import ca.mcmaster.sploadbalancexy.heuristics.AveragingHeuristic;
import static ca.mcmaster.sptestdriverxy.Parameters.MAX_ITERATIONS;
import static ca.mcmaster.sptestdriverxy.Parameters.*;
import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.*;
import java.util.Map;
import org.apache.log4j.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 *
 * @author srini
 */
public class SparkSimulatorDriver {
    
    private static Logger logger=Logger.getLogger(SparkSimulatorDriver.class);
    private static    List<ActiveSubtreeCollection> partitionList = new ArrayList<ActiveSubtreeCollection>(NUM_PARTITIONS);
     
    
    public SparkSimulatorDriver() {
                       
    }
    
    public static void main(String[] args) throws Exception {
        Solution incumbent = new Solution();
        
        logger.setLevel(Level.DEBUG);
        PatternLayout layout = new PatternLayout("%5p  %d  %F  %L  %m%n");     
        logger.addAppender(new RollingFileAppender(layout,LOG_FOLDER+SparkSimulatorDriver.class.getSimpleName()+""+ LOG_FILE_EXTENSION));
        logger.debug ("Spark simulator version 1.0" );
        
        //init the partitions
        for (int index = ZERO; index < NUM_PARTITIONS; index ++) {
            if (index==ZERO){
                ActiveSubtreeCollection astc = new ActiveSubtreeCollection();
                astc.add(new NodeAttachment());
                partitionList.add(astc);
            }else {
                partitionList.add(new ActiveSubtreeCollection());
            }
        }
        
        //solve these partitions forever , until all partitions are empty or an iteration limit is reached
        for (int iteration = ZERO;iteration<MAX_ITERATIONS && !isHaltFilePresent();iteration++){
            
            if (allPartitionsEmpty()) break;
            
            //abort computation in case of error
            if (incumbent.isError() || incumbent.isUnbounded()) break;
            
            logger.info(NEWLINE+NEWLINE+" Starting iteration "+iteration+ NEWLINE+NEWLINE);
            int iterationDuration = getSparkCycleTime(iteration);
            
            if (iteration == ZERO) {
                MAXIMUM_LEAF_NODES_PER_SUB_TREE  =  MAXIMUM_LEAF_NODES_PER_SUB_TREE_RAMPUP;
                MINIMUM_LEAF_NODES_PER_SUB_TREE  =  MINIMUM_LEAF_NODES_PER_SUB_TREE_RAMPUP; 
     
            } else {
                MAXIMUM_LEAF_NODES_PER_SUB_TREE  =  MAXIMUM_LEAF_NODES_PER_SUB_TREE_REGULAR;
                MINIMUM_LEAF_NODES_PER_SUB_TREE  =  MINIMUM_LEAF_NODES_PER_SUB_TREE_REGULAR; 
    
            }
            
            //solve each partition for cycle time
            for (int index = ZERO; index < NUM_PARTITIONS; index ++) {
                ActiveSubtreeCollection astc = partitionList.get(index);
                logger.info("Solving partition "+index + " having "+astc.getNumberOFTrees() + " trees with a total of " + astc.getNumberOFLeafsAcrossAllTrees()+
                        " leafs and this many raw nodes "+astc.getRawNodesCount());
                 
                astc.solve(Instant.now().plusMillis(THOUSAND*iterationDuration) , incumbent, iteration,index );
                
            }
            
            if (isHaltFilePresent()) break;
            
            //update incumbent
            for (int index = ZERO; index < NUM_PARTITIONS; index ++) {
                Solution partitionSolution = partitionList.get(index).getSolution() ;
                if ( ZERO != (new SolutionComparator()).compare(incumbent, partitionSolution)){
                    //we have found a better solution

                    //update our copies
                    incumbent = partitionSolution;  
                    logger.info("Best incumbent updated to "+incumbent.getObjectiveValue());
                    
                    //we will abort solution process if error
                    if (incumbent.isError() ||incumbent.isUnbounded())  break;

                }

            }
            
            //we will abort solution process if error
            if (incumbent.isError() ||incumbent.isUnbounded())  break;
             
            if (isHaltFilePresent()) break;
            
            //clean up partitions of inferrior subtrees, now that we have the updated incumbent
            for (int index = ZERO; index < NUM_PARTITIONS; index ++) {
                ActiveSubtreeCollection astc = partitionList.get(index);
                astc.cullTrees(incumbent.getObjectiveValue() );
                astc.removeInferiorRawNodes(incumbent.getObjectiveValue() );
            }


            
            //do load balancing if needed
            AveragingHeuristic loadBalancer = new AveragingHeuristic(partitionList );            
            if (loadBalancer.isLoadBalancingRequired()) {
                
                loadBalancer.loadBalance();
                
                
            } else {
                logger.info("load balance or raw nodes not needed ");
            }
           
             
            //do the next iteration
            
        }//end for 1000 iterations
        
        //print solution
        logger.info("Best soln found "+incumbent);
        
        //timtab 1 -> 764772
        //markshare 5 0 ->1
        //a1c1s1 -> 11503.4
        //timtab2 -> 1096557
         
        //print time usage % for every partition
        for ( ActiveSubtreeCollection astc : partitionList){
            logger.info("Time slice utilization percent = " + TEN*TEN*astc.totalTimeUsedForSolving/astc.totalTimeAllocatedForSolving);
        }
        
    }//end main
  

    
    private static int getSparkCycleTime (int iteration) {
        
        int cycleTime = iteration>ZERO?  SPARK_ITERATION_CYCLE_TIME : SPARK_RAMP_UP_TIME_IN_SECONDS;
        //logger.debug("Spark cycle time will be "+cycleTime);
        return cycleTime;
    }
    
    
    
    private static boolean allPartitionsEmpty( ) throws Exception {
        boolean retval = true;
        for (int index = ZERO; index < NUM_PARTITIONS; index ++) {
             
            if ( partitionList.get(index).getNumberOFTrees()>ZERO || partitionList.get(index).getRawNodesCount()>ZERO ){
                retval = false;
                break;
            }
        }
             
        return retval;
    }
    
    private static boolean isHaltFilePresent (){
        File file = new File(HALT_FILE);
        return file.exists();
    }
}
