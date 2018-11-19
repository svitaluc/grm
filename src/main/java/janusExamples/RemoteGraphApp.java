package janusExamples;// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import helpers.ProcessedResultLogGraphFactory;
import logHandling.LogFileLoader;
import logHandling.MyLog;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

public class RemoteGraphApp extends JanusGraphApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteGraphApp.class);

    // used for bindings
    private static final String NAME = "name";
    private static final String AGE = "age";
    private static final String TIME = "time";
    private static final String REASON = "reason";
    private static final String PLACE = "place";
    private static final String LABEL = "label";
    private static final String OUT_V = "outV";
    private static final String IN_V = "inV";

    protected JanusGraph janusgraph;
    protected Cluster cluster;
    protected Client client;
    protected Configuration conf;

    /**
     * Constructs a graph app using the given properties.
     *
     * @param fileName location of the properties file
     */
    public RemoteGraphApp(final String fileName) {
        super(fileName);
        // the server auto-commits per request, so the application code doesn't
        // need to explicitly commit transactions
        this.supportsTransactions = false;
    }

    @Override
    public GraphTraversalSource openGraph() throws ConfigurationException {
        LOGGER.info("opening graph");
        conf = new PropertiesConfiguration(propFileName);

        // using the remote driver for schema
        try {
            cluster = Cluster.open(conf.getString("gremlin.remote.driver.clusterFile"));
            client = cluster.connect();
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }

        // using the remote graph for queries
        graph = EmptyGraph.instance();
        g = JanusGraphFactory.open("inmemory").traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
        return g;
    }

    @Override
    public void createElements() {
        LOGGER.info("creating elements");
        try {
            MyLog log = LogFileLoader.load(new File("log"));
            ProcessedResultLogGraphFactory.createElements(g, log);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    @Override
    public void closeGraph() throws Exception {
        LOGGER.info("closing graph");
        try {
            if (g != null) {
                // this closes the remote, no need to close the empty graph
                g.close();
            }
            if (cluster != null) {
                // the cluster closes all of its clients
                cluster.close();
            }
        } finally {
            g = null;
            graph = null;
            client = null;
            cluster = null;
        }
    }

    @Override
    public void createSchema() {
        LOGGER.info("creating schema");
        // get the schema request as a string
        final String req = createSchemaRequest();
        // submit the request to the server
        final ResultSet resultSet = client.submit(req);
        // drain the results completely
        Stream<Result> futureList = resultSet.stream();
        futureList.map(Result::toString).forEach(LOGGER::info);
    }

    public static void main(String[] args) {
        final String fileName = (args != null && args.length > 0) ? args[0] : null;
        final RemoteGraphApp app = new RemoteGraphApp(fileName);
        app.runApp();
    }
}