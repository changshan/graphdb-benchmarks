//package org.neo4j.spark
//
//import org.apache.spark.api.java.JavaSparkContext
//import org.apache.spark.graphx.{Edge, Graph}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
//import org.apache.spark.sql.{DataFrame, Row, SQLContext}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.junit.Assert._
//import org.junit._
//import org.neo4j.graphdb.ResourceIterator
//import org.neo4j.harness.{ServerControls, TestServerBuilders}
//
//import scala.collection.JavaConverters._
//
//
///**
//  * @author mh
//  * @since 17.07.16
//  */
//class Neo4jGraphX {
//  val FIXTURE: String = "CREATE (:A)-[:REL {foo:'bar'}]->(:B)"
//  private var conf: SparkConf = null
//  private var sc: JavaSparkContext = null
//  private var server: ServerControls = null
//
//  @Before
//  @throws[Exception]
//  def setUp {
//    server = TestServerBuilders.newInProcessBuilder.withConfig("dbms.security.auth_enabled", "false").withFixture(FIXTURE).newServer
//    conf = new SparkConf().setAppName("neoTest").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", server.boltURI.toString)
//    sc = SparkContext.getOrCreate(conf)
//  }
//
//  @After def tearDown {
//    server.close
//    sc.close
//  }
//
//  @Test def mergeEdgeList {
//    val rows = sc.makeRDD(Seq(Row("Keanu", "Matrix")))
//    val schema = StructType(Seq(StructField("name", DataTypes.StringType), StructField("title", DataTypes.StringType)))
//    val df = new SQLContext(sc).createDataFrame(rows, schema)
//    Neo4jDataFrame.mergeEdgeList(sc, df, ("Person",Seq("name")),("ACTED_IN",Seq.empty),("Movie",Seq("title")))
//
//    val it: ResourceIterator[Long] = server.graph().execute("MATCH (:Person {name:'Keanu'})-[:ACTED_IN]->(:Movie {title:'Matrix'}) RETURN count(*) as c").columnAs("c")
//    assertEquals(1L, it.next())
//    it.close()
//  }
//  
//  @Test def shortPaths{
//    
//    val shortestPaths = Set(
//        (1, Map(1 -> 0, 4 -> 2)), (2, Map(1 -> 1, 4 -> 2)), (3, Map(1 -> 2, 4 -> 1)),
//        (4, Map(1 -> 2, 4 -> 0)), (5, Map(1 -> 1, 4 -> 1)), (6, Map(1 -> 3, 4 -> 1)))
//
//        // ��������ͼ�ı�����
//        val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
//            case e => Seq(e, e.swap)
//        }
//
//        // ��������ͼ
//        val edges = sc.parallelize(edgeSeq).map { case (v1, v2) => (v1.toLong, v2.toLong) }
//        val graph = Graph.fromEdgeTuples(edges, 1)
//
//        // Ҫ�����·���ĵ㼯��
//        val landmarks = Seq(1, 4).map(_.toLong)
//
//        // �������·��
//        val results = ShortestPaths.run(graph, landmarks).vertices.collect.map {
//        case (v, spMap) => (v, spMap.mapValues(i => i))
//        }
//        // ����ʵ����Ա�
//        assert(results.toSet === shortestPaths)
//  }
//  
//  
//}
