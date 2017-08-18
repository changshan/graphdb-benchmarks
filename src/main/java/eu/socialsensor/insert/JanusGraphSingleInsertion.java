package eu.socialsensor.insert;

import java.io.File;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.util.JanusGraphId;
import org.janusgraph.graphdb.query.JanusGraphPredicate.Converter;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;

import eu.socialsensor.graphdatabases.GraphDatabaseBase;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of single Insertion in Titan graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class JanusGraphSingleInsertion extends InsertionBase<Vertex>
{
    private final JanusGraph janusGraph;

    public JanusGraphSingleInsertion(JanusGraph janusGraph, GraphDatabaseType type, File resultsPath)
    {
        super(type, resultsPath);
        this.janusGraph = janusGraph;
    }

	@Override
    public Vertex getOrCreate(String value)
    {
        Integer intValue = Integer.valueOf(value);
        Vertex v = null;
        if (janusGraph.query().has(GraphDatabaseBase.NODE_ID).vertices().iterator().hasNext())
        {
            v = (Vertex) janusGraph.query().has(GraphDatabaseBase.NODE_ID).vertices().iterator().next();
        }
        else
        {
            final long janusGraphId = JanusGraphId.toVertexId(intValue);
            v.setProperty(GraphDatabaseBase.NODE_ID, janusGraphId);
            janusGraph.tx().commit();
        }
        
        return v;
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {
        try
        {
        	src.addEdge(GraphDatabaseBase.SIMILAR, dest);
        	janusGraph.tx().commit();
        }
        catch (Exception e)
        {
        	janusGraph.tx().rollback(); //TODO(amcp) why can this happen? doesn't this indicate illegal state?
        }
    }
}
