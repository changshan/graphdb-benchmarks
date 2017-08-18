package eu.socialsensor.insert;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.internal.CommunityCompatibilityFactory;
import org.neo4j.cypher.internal.CompatibilityFactory;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import eu.socialsensor.graphdatabases.GraphDatabaseBase;
import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of single Insertion in Neo4j graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
@SuppressWarnings("deprecation")
public class Neo4jSingleInsertion extends InsertionBase<Node>
{
    private final GraphDatabaseService neo4jGraph;
    private final ExecutionEngine engine;

    public Neo4jSingleInsertion(GraphDatabaseService neo4jGraph, File resultsPath)
    {
        super(GraphDatabaseType.NEO4J, resultsPath);
        this.neo4jGraph = neo4jGraph;
        GraphDatabaseCypherService queryService=new GraphDatabaseCypherService(neo4jGraph);
        LogProvider logProvider = NullLogProvider.getInstance();
        CommunityCompatibilityFactory compatibilityFactory =
                new CommunityCompatibilityFactory( queryService, null, null, logProvider );
        engine = new ExecutionEngine(queryService,logProvider,compatibilityFactory);
        
    }

    public Node getOrCreate(String nodeId)
    {
        Node result = null;
        
        
        try(final Transaction tx = ((GraphDatabaseAPI) neo4jGraph).beginTx())
        {
            try
            {
                String queryString = "MERGE (n:Node {nodeId: {nodeId}}) RETURN n";
                Map<String, Object> parameters = new HashMap<String, Object>();
                parameters.put(GraphDatabaseBase.NODE_ID, nodeId);
                ResourceIterator<Node> resultIterator = neo4jGraph.execute(queryString, parameters).columnAs("n");
                result = resultIterator.next();
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get or create node " + nodeId, e);
            }
        }

        return result;
    }

    @Override
    public void relateNodes(Node src, Node dest)
    {
        try (final Transaction tx = ((GraphDatabaseAPI) neo4jGraph).beginTx())
        {
            try
            {
                 src.createRelationshipTo(dest, Neo4jGraphDatabase.RelTypes.SIMILAR);
                
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to relate nodes", e);
            }
        }
    }
}
