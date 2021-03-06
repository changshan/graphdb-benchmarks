package eu.socialsensor.graphdatabases;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.ShortestPaths;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.spark.Neo4jGraph;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.Neo4jMassiveInsertion;
import eu.socialsensor.insert.Neo4jSingleInsertion;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Neo4JavaSparkContext;
import eu.socialsensor.utils.Utils;
import scala.collection.Seq;
import scala.collection.Seq$;

/**
 * Neo4j graph database implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class Neo4jGraphDatabase extends GraphDatabaseBase<Iterator<Node>, Iterator<Relationship>, Node, Relationship>
{
    protected GraphDatabaseService neo4jGraph = null;
    private Schema schema = null;
    
    public static final String QUERY = "MATCH (n:Node}) RETURN n";

    private BatchInserter inserter = null;

    public static enum RelTypes implements RelationshipType
    {
        SIMILAR
    }

    public static Label NODE_LABEL = DynamicLabel.label("Node");

    public Neo4jGraphDatabase(File dbStorageDirectoryIn)
    {
        super(GraphDatabaseType.NEO4J, dbStorageDirectoryIn);
    }

    @Override
    public void open()
    {
        neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbStorageDirectory.getAbsolutePath()));
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                neo4jGraph.schema().awaitIndexesOnline(10l, TimeUnit.MINUTES);
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unknown error", e);
            }
        }
    }

    @Override
    public void createGraphForSingleLoad()
    {
        neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabase(new File((dbStorageDirectory.getAbsolutePath())));
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                schema = neo4jGraph.schema();
                schema.indexFor(NODE_LABEL).on(NODE_ID).create();
                schema.indexFor(NODE_LABEL).on(COMMUNITY).create();
                schema.indexFor(NODE_LABEL).on(NODE_COMMUNITY).create();
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unknown error", e);
            }
        }
    }

    @Override
    public void createGraphForMassiveLoad()
    {
        Map<String, String> config = new HashMap<String, String>();
        config.put("cache_type", "none");
        config.put("use_memory_mapped_buffers", "true");
        config.put("neostore.nodestore.db.mapped_memory", "200M");
        config.put("neostore.relationshipstore.db.mapped_memory", "1000M");
        config.put("neostore.propertystore.db.mapped_memory", "250M");
        config.put("neostore.propertystore.db.strings.mapped_memory", "250M");

        try {
			inserter = BatchInserters.inserter(new File(dbStorageDirectory.getAbsolutePath()), config);
		} catch (IOException e) {
			e.printStackTrace();
		}
        createDeferredSchema();
    }

    private void createDeferredSchema()
    {
        inserter.createDeferredSchemaIndex(NODE_LABEL).on(NODE_ID).create();
        inserter.createDeferredSchemaIndex(NODE_LABEL).on(COMMUNITY).create();
        inserter.createDeferredSchemaIndex(NODE_LABEL).on(NODE_COMMUNITY).create();
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber,Integer blocks)
    {
        Insertion neo4jSingleInsertion = new Neo4jSingleInsertion(this.neo4jGraph, resultsPath);
        neo4jSingleInsertion.createGraph(dataPath, scenarioNumber,blocks);
    }

    @Override
    public void massiveModeLoading(File dataPath)
    {
        Insertion neo4jMassiveInsertion = new Neo4jMassiveInsertion(this.inserter);
        neo4jMassiveInsertion.createGraph(dataPath, 0 /* scenarioNumber */);
    }

    @Override
    public void shutdown()
    {
        if (neo4jGraph == null)
        {
            return;
        }
        neo4jGraph.shutdown();
    }

    @Override
    public void delete()
    {
        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph()
    {
        if (inserter == null)
        {
            return;
        }
        inserter.shutdown();

        File store_lock = new File("graphDBs/Neo4j", "store_lock");
        store_lock.delete();
        if (store_lock.exists())
        {
            throw new BenchmarkingException("could not remove store_lock");
        }

        File lock = new File("graphDBs/Neo4j", "lock");
        lock.delete();
        if (lock.exists())
        {
            throw new BenchmarkingException("could not remove lock");
        }

        inserter = null;
    }
    @Override
    public void shortestPath(Node n1, Integer i,Boolean sparkGrouphX)
    {
    	
    	if(sparkGrouphX){
    		//使用sparkGrouphX计算结果
    		SparkConf conf;
    	    JavaSparkContext sc;
    	    Neo4JavaSparkContext csc;
    	    ServerControls server;
    	    server = TestServerBuilders.newInProcessBuilder()
                    .withFixture(QUERY)
                    .newServer();
            conf = new SparkConf()
                    .setAppName("neoTest")
                    //配置多少个节点
                    .setMaster("local[*]")
                    .set("spark.driver.allowMultipleContexts","true")
                    .set("spark.neo4j.bolt.url", server.boltURI().toString());
            sc = new JavaSparkContext(conf);
            csc = Neo4JavaSparkContext.neo4jContext(sc);
            
            //获取相邻的节点
            Set<Integer> set = getNeighborsIds(i);
            
            // 构造有向图的边序列
            Dataset<Row> found = csc.queryDF(QUERY, null,"nodeId", "integer");

            // 构造有向图
            Graph graph = Neo4jGraph.loadGraph(sc.sc(), "A", null, "B");
            
            // 要求最短路径的点集合
            
            // 计算最短路径
            Seq<Object> empty = (Seq<Object>) Seq$.MODULE$.empty();
           
            ShortestPaths.run(graph, empty,null);

            server.close();
            sc.close();
            
    	}else{
    		//使用自己的算法求最短路径
            PathFinder<Path> finder= GraphAlgoFactory.shortestPath(PathExpanders.forType(Neo4jGraphDatabase.RelTypes.SIMILAR), 5);
            Node n2 = getVertex(i);
            Path path = finder.findSinglePath(n1, n2);
    	}
    }

    //TODO can unforced option be pulled into configuration?
    private Transaction beginUnforcedTransaction() {
        return ((GraphDatabaseAPI) neo4jGraph).beginTx();
    }

    @Override
    public int getNodeCount()
    {
        Long nodeCount = 0L;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                nodeCount = Iterators.count(neo4jGraph.getAllNodes().iterator());
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get node count", e);
            }
        }

        return nodeCount.intValue();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        Set<Integer> neighbors = new HashSet<Integer>();
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                Node n = neo4jGraph.findNodes(NODE_LABEL, NODE_ID, String.valueOf(nodeId)).next();
                for (Relationship relationship : n.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING))
                {
                    Node neighbour = relationship.getOtherNode(n);
                    String neighbourId = (String) neighbour.getProperty(NODE_ID);
                    neighbors.add(Integer.valueOf(neighbourId));
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get neighbors ids", e);
            }
        }

        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId)
    {
        double weight = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                Node n = neo4jGraph.findNodes(NODE_LABEL, NODE_ID, String.valueOf(nodeId)).next();
                weight = getNodeOutDegree(n);
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get node weight", e);
            }
        }

        return weight;
    }

    public double getNodeInDegree(Node node)
    {
        Iterable<Relationship> rel = node.getRelationships(Direction.OUTGOING, RelTypes.SIMILAR);
        return (double) (Iterators.count(rel.iterator()));
    }

    public double getNodeOutDegree(Node node)
    {
        Iterable<Relationship> rel = node.getRelationships(Direction.INCOMING, RelTypes.SIMILAR);
        return (double) (Iterators.count(rel.iterator()));
    }

    @Override
    public void initCommunityProperty()
    {
        int communityCounter = 0;

        // maybe commit changes every 1000 transactions?
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                for (Node n : neo4jGraph.getAllNodes())
                {
                    n.setProperty(NODE_COMMUNITY, communityCounter);
                    n.setProperty(COMMUNITY, communityCounter);
                    communityCounter++;
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to initialize community property", e);
            }
        }
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities)
    {
        Set<Integer> communities = new HashSet<Integer>();
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
            	ResourceIterator<Node> nodes = neo4jGraph.findNodes(Neo4jGraphDatabase.NODE_LABEL,
                    NODE_COMMUNITY, nodeCommunities);
                while(nodes.hasNext())
                {
                    Node n = nodes.next();
					for (Relationship r : n.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING))
                    {
                        Node neighbour = r.getOtherNode(n);
                        Integer community = (Integer) (neighbour.getProperty(COMMUNITY));
                        communities.add(community);
                    }
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get communities connected to node communities", e);
            }
        }

        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
              	ResourceIterator<Node> nodeitr = neo4jGraph.findNodes(NODE_LABEL, COMMUNITY, community);
                	while(nodeitr.hasNext())
                {
                    String nodeIdString = (String) (nodeitr.next().getProperty(NODE_ID));
                    nodes.add(Integer.valueOf(nodeIdString));
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get nodes from community", e);
            }
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodes = new HashSet<Integer>();

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
            	ResourceIterator<Node> nodeitr = neo4jGraph.findNodes(NODE_LABEL, NODE_COMMUNITY,
                    nodeCommunity);
                	while(nodeitr.hasNext())
                {
                    String nodeIdString = (String) (nodeitr.next().getProperty(NODE_ID));
                    nodes.add(Integer.valueOf(nodeIdString));
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get nodes from node community", e);
            }
        }

        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes)
    {
        double edges = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
            	ResourceIterator<Node> nodes = neo4jGraph.findNodes(NODE_LABEL, NODE_COMMUNITY,
                    nodeCommunity);
            	ResourceIterator<Node> comNodes = neo4jGraph.findNodes(NODE_LABEL, COMMUNITY,
                    communityNodes);
               while(nodes.hasNext())
                {
                    Node node = nodes.next();
					Iterable<Relationship> relationships = node.getRelationships(RelTypes.SIMILAR, Direction.OUTGOING);
                    for (Relationship r : relationships)
                    {
                        Node neighbor = r.getOtherNode(node);
                        if (Iterators.contains(comNodes, neighbor))
                        {
                            edges++;
                        }
                    }
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get edges inside community", e);
            }
        }

        return edges;
    }

    @Override
    public double getCommunityWeight(int community)
    {
        double communityWeight = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterator<Node> iter = neo4jGraph.findNodes(NODE_LABEL, COMMUNITY, community);
                if (Iterators.count(iter) > 1)
                {
                    while(iter.hasNext())
                    {
                        communityWeight += getNodeOutDegree(iter.next());
                    }
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get community weight", e);
            }
        }

        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        double nodeCommunityWeight = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterator<Node> iter = neo4jGraph.findNodes(NODE_LABEL, NODE_COMMUNITY,
                    nodeCommunity);
                if (Iterators.count(iter) > 1)
                {
                    while(iter.hasNext())
                    {
                        nodeCommunityWeight += getNodeOutDegree(iter.next());
                    }
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get node community weight", e);
            }
        }

        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int nodeCommunity, int toCommunity)
    {
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                ResourceIterator<Node> fromIter = neo4jGraph.findNodes(NODE_LABEL, NODE_COMMUNITY,nodeCommunity);
                while (fromIter.hasNext())
                {
                	fromIter.next().setProperty(COMMUNITY, toCommunity);
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to move node", e);
            }
        }
    }

    @Override
    public double getGraphWeightSum()
    {
        Long edgeCount = 0L;

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                edgeCount =Iterators.count(neo4jGraph.getAllRelationships().iterator());
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get graph weight sum", e);
            }
        }

        return (double) edgeCount;
    }

    @Override
    public int reInitializeCommunities()
    {
        Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
        int communityCounter = 0;

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                for (Node n : neo4jGraph.getAllNodes())
                {
                    Integer communityId = (Integer) (n.getProperty(COMMUNITY));
                    if (!initCommunities.containsKey(communityId))
                    {
                        initCommunities.put(communityId, communityCounter);
                        communityCounter++;
                    }
                    int newCommunityId = initCommunities.get(communityId);
                    n.setProperty(COMMUNITY, newCommunityId);
                    n.setProperty(NODE_COMMUNITY, newCommunityId);
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to reinitialize communities", e);
            }
        }

        return communityCounter;
    }

    @Override
    public int getCommunity(int nodeCommunity)
    {
        Integer community = 0;

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
           	 ResourceIterator<Node> nodesIter = neo4jGraph.findNodes(NODE_LABEL, COMMUNITY, community);
             Node node =nodesIter.next();
                community = (Integer) (node.getProperty(COMMUNITY));
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get community", e);
            }
        }

        return community;
    }

    @Override
    public int getCommunityFromNode(int nodeId)
    {
        Integer community = 0;
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
            	 ResourceIterator<Node> nodesIter = neo4jGraph.findNodes(NODE_LABEL, COMMUNITY, community);
                Node node =nodesIter.next();
                community = (Integer) (node.getProperty(COMMUNITY));
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get community from node", e);
            }
        }

        return community;
    }

    @Override
    public int getCommunitySize(int community)
    {
        Set<Integer> nodeCommunities = new HashSet<Integer>();

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
            	 ResourceIterator<Node> nodesIter = neo4jGraph.findNodes(NODE_LABEL, COMMUNITY, community);
            	 while(nodesIter.hasNext())
                {
                    Integer nodeCommunity = (Integer) (nodesIter.next().getProperty(COMMUNITY));
                    nodeCommunities.add(nodeCommunity);
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to get community size", e);
            }
        }

        return nodeCommunities.size();
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities)
    {
        Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();

        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
                for (int i = 0; i < numberOfCommunities; i++)
                {
               	 ResourceIterator<Node> nodesIter = neo4jGraph.findNodes(NODE_LABEL, COMMUNITY, i);
                    List<Integer> nodes = new ArrayList<Integer>();
                    while(nodesIter.hasNext())
                    {
                        String nodeIdString = (String) (nodesIter.next().getProperty(NODE_ID));
                        nodes.add(Integer.valueOf(nodeIdString));
                    }
                    communities.put(i, nodes);
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to map communities", e);
            }
        }

        return communities;
    }

    @Override
    public boolean nodeExists(int nodeId)
    {
        try (final Transaction tx = beginUnforcedTransaction())
        {
            try
            {
            	 ResourceIterator<Node> nodesIter = neo4jGraph.findNodes(NODE_LABEL, NODE_ID, nodeId);
                if (nodesIter.hasNext())
                {
                    tx.success();
                    return true;
                }
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to determine if node exists", e);
            }
        }
        return false;
    }

    @Override
    public Iterator<Node> getVertexIterator()
    {
        return neo4jGraph.getAllNodes().iterator();
    }

    @Override
    public Iterator<Relationship> getNeighborsOfVertex(Node v)
    {
        return v.getRelationships(Neo4jGraphDatabase.RelTypes.SIMILAR, Direction.BOTH).iterator();
    }

    @Override
    public void cleanupVertexIterator(Iterator<Node> it)
    {
        // NOOP
    }

    @Override
    public Node getOtherVertexFromEdge(Relationship r, Node n)
    {
        return r.getOtherNode(n);
    }

    @Override
    public Iterator<Relationship> getAllEdges()
    {
        return neo4jGraph.getAllRelationships().iterator();
    }

    @Override
    public Node getSrcVertexFromEdge(Relationship edge)
    {
        return edge.getStartNode();
    }

    @Override
    public Node getDestVertexFromEdge(Relationship edge)
    {
        return edge.getEndNode();
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Relationship> it)
    {
        return it.hasNext();
    }

    @Override
    public Relationship nextEdge(Iterator<Relationship> it)
    {
        return it.next();
    }

    @Override
    public void cleanupEdgeIterator(Iterator<Relationship> it)
    {
        //NOOP
    }

    @Override
    public boolean vertexIteratorHasNext(Iterator<Node> it)
    {
        return it.hasNext();
    }

    @Override
    public Node nextVertex(Iterator<Node> it)
    {
        return it.next();
    }

    @Override
    public Node getVertex(Integer i)
    {
        // note, this probably should be run in the context of an active transaction.
        return neo4jGraph.findNodes(Neo4jGraphDatabase.NODE_LABEL, NODE_ID, i.toString()).next();
    }

}
