package eu.socialsensor.insert;

import java.io.File;
import java.util.List;

/**
 * Represents the insertion of data in each graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 */
public interface Insertion
{

    /**
     * Loads the data in each graph database
     * 
     * @param datasetDir
     */
    public void createGraph(File dataset, int scenarioNumber,Integer blocks);
    
    public void createGraph(File dataset, int scenarioNumber);

}
