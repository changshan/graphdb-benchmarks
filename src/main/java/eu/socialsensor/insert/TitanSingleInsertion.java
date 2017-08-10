package eu.socialsensor.insert;

import java.io.File;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.util.JanusGraphId;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of single Insertion in Titan graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class TitanSingleInsertion extends InsertionBase<Vertex>
{
    private final JanusGraph titanGraph;

    public TitanSingleInsertion(JanusGraph titanGraph, GraphDatabaseType type, File resultsPath)
    {
        super(type, resultsPath);
        this.titanGraph = titanGraph;
    }

    @Override
    public Vertex getOrCreate(String value)
    {
        Integer intValue = Integer.valueOf(value);
        final Vertex v;
        if (titanGraph.query().has("nodeId", Compare.EQUAL, intValue).vertices().iterator().hasNext())
        {
            v = (Vertex) titanGraph.query().has("nodeId", Compare.EQUAL, intValue).vertices().iterator().next();
        }
        else
        {
            final long titanVertexId = JanusGraphId.toVertexId(intValue);
            v.setProperty("nodeId", intValue);
            titanGraph.tx().commit();
        }
        return v;
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {
        try
        {
        	//titanGraph.addEdge(null, src, dest, "similar");
        	//titanGraph.addVertex("similar");
        	src.addEdge("similar", dest);
            titanGraph.tx().commit();
        }
        catch (Exception e)
        {
            titanGraph.tx().rollback(); //TODO(amcp) why can this happen? doesn't this indicate illegal state?
        }
    }
}
