package eu.socialsensor.insert;

import org.janusgraph.core.util.JanusGraphId;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import eu.socialsensor.graphdatabases.GraphDatabaseBase;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of massive Insertion in Titan graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class JanusGraphMassiveInsertion extends InsertionBase<Vertex>
{
    @SuppressWarnings("rawtypes")
	private final BatchGraph batchGraph;

    public JanusGraphMassiveInsertion(BatchGraph batchGraph, GraphDatabaseType type)
    {
        super(type, null /* resultsPath */); // no temp files for massive load
                                             // insert
        this.batchGraph = batchGraph;
    }

    @Override
    public Vertex getOrCreate(String value)
    {
        Integer intVal = Integer.valueOf(value);
        final long janusGraphId = JanusGraphId.toVertexId(intVal);
        Vertex vertex = batchGraph.getVertex(janusGraphId);
        if (vertex == null)
        {
            vertex = batchGraph.addVertex(janusGraphId);
            vertex.setProperty(GraphDatabaseBase.NODE_ID, intVal);
        }
        return vertex;
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {
        src.addEdge(GraphDatabaseBase.SIMILAR, dest);
    }
}
