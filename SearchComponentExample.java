package com.ccih.common.search.plugins;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Flow:
 * -----
 * 1. When the Solr core initialized the init() method is called.
 * 2. When a new query arrives to Solr, the RequestHandler invokes the SearchHandler.handleRequestBody() method.
 *    2.11 SearchHandler in its handleRequestBody() calls in a loop to all SearchComponent.prepare().
 * 3. Then, for each stage the SearchHandler calls to -> distributedProcess() -> handleResponses()-> finishStage() of each SearchComponent.
 *
 *  There are 4 stages to distributed search:
 *  -----------------------------------------
 *  Start (ResponseBuilder.STAGE_START)
 *  Query Parse (ResponseBuilder.STAGE_PARSE_QUERY)
 *  Execute Query (ResponseBuilder.STAGE_EXECUTE_QUERY)
 *  Get Fields (ResponseBuilder.STAGE_GET_FIELDS)
 *  (There's also ResponseBuilder.STAGE_DONE, which should be returned when the component has nothing left to do.)
 *
 *
 * Detailed explanation about distributed process flow:
 * ----------------------------------------------------
 * - Aggregator distributedProcess() method is called for each stage in the query (see above).
 *   - In the below example, once the EXECUTE_QUERY stage is called, the aggregator generated a ShardRequest object and add it to the outgoing requests queue.
 *   - The SearchHandler checks if the queue is full and send a NON distributed query to the shards by 'params.set(CommonParams.DISTRIB, "false");' (see the line 'while (rb.outgoing.size() > 0)').
 *      - Each shard gets the call and execute its own logic in the process() method (the aggregator do the same) and return it's response to the aggregator.
 *   - Once all the shards finished, in the aggregator, the SearchHandler pass the shard responses by invoking the handleResponses(shardResponses) method (for example: at this stage the QuerySearchComponent merge the results).
 *   - Finally the finishStage() invoked for each stage - usually in order to fill the response to the client.
 *
 *   ***In non distributed mode, only the process() method is called (see  'if (!rb.isDistrib)' in SearchHandler.handleRequestBody() ) ***
 *
 * In order to make this SearchComponent work add the below snippet to a request handler:
 * ---------------------------------------------------------------------------------------
 *      <requestHandler ...>
 *          ....
 *          <arr name="last-components">
 *              <str>trend</str>
 *          </arr>
 *      </requestHandler>
 *
 * And add the SearchComponent definition:
 * ----------------------------------------
 *      <searchComponent name="trend" class="com.ccih.common.search.plugins.TrendSearchComponent">
 *          <str name="param">someParam</str>
 *      </searchComponent>
 *
 *
 * References:
 * -----------
 *   http://wiki.apache.org/solr/WritingDistributedSearchComponents
 *   http://www.slideshare.net/searchbox-com/tutorial-on-developin-a-solr-search-component-plugin
 *   {@link org.apache.solr.handler.component.QueryComponent}
 *   {@link org.apache.solr.handler.component.HighlightComponent}
 */
public class TrendSearchComponent extends SearchComponent{

    protected static       Logger logger  = LoggerFactory.getLogger(TrendSearchComponent.class);

    protected final int    TREND_SHARD_REQUEST = 0x1001;
    protected final String TRENDS_ON = "trend";

    protected int          total = 0;
    protected String       parameter;

    /**
     * Called during the Solr core initialization.
     */
    @Override
    public void init(NamedList args){
        super.init(args);

        // Read solrconfig.xml parameter value (see definition above)
        String paramValue = (String) args.get("param");
        if (StringUtils.isBlank(paramValue)){
            parameter = paramValue;
        }
    }

    /**
     * This method will be executed for each stage, before the distributedProcess().
     * @param rb
     * @throws IOException
     */
    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Process for a distributed search.
     * @return the next stage for this component
     */
    @Override
    public int distributedProcess(ResponseBuilder rb) throws IOException {

        if (rb.stage < ResponseBuilder.STAGE_PARSE_QUERY)
            return ResponseBuilder.STAGE_PARSE_QUERY;
        if (rb.stage == ResponseBuilder.STAGE_PARSE_QUERY) {
            return ResponseBuilder.STAGE_EXECUTE_QUERY;
        }
        if (rb.stage < ResponseBuilder.STAGE_EXECUTE_QUERY) return ResponseBuilder.STAGE_EXECUTE_QUERY;
        if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
            // At this stage the QuerySearchComponent gets the IDs from the other shards
            // Here we can send data to shards (i.e configuration).
            SolrCore.log.info("Aggregator only, distributedProcess STAGE_EXECUTE_QUERY");
            createShardsRequest(rb, TREND_SHARD_REQUEST);
            return ResponseBuilder.STAGE_GET_FIELDS;
        }
        if (rb.stage < ResponseBuilder.STAGE_GET_FIELDS) return ResponseBuilder.STAGE_GET_FIELDS;
        if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
            //createShardsRequest(rb, 1);
            return ResponseBuilder.STAGE_DONE;
        }
        return ResponseBuilder.STAGE_DONE;
    }

    /* Create new request to other shards and add it to the queue
       The propose is a flag that tells to the TrendSearchComponent in the shards to run or not ()
    * */
    private void createShardsRequest(ResponseBuilder rb, int purpose){
        SolrCore.log.info("Aggregator only, createShardsRequest()");

        ShardRequest sreq = new ShardRequest();
        sreq.purpose = purpose; //this purpose is read by the finishStage() method later on.

        sreq.params = new ModifiableSolrParams(rb.req.getParams());

        //Pass additional parameter to the Shards, they will use this parameter to understand what to do (see process())
        sreq.params.set(TRENDS_ON,"true");

        rb.addRequest(this, sreq); // add the request to the outgoing queue
    }

    /**
     * This method will run on each and every shard after the distributedProcess() method in the aggregator
     */
    @Override
    public void process(ResponseBuilder rb) throws IOException {
        SolrQueryRequest req = rb.req;
        SolrQueryResponse rsp = rb.rsp;

        SolrParams params = req.getParams();
        if (params.getBool(TRENDS_ON,false)) {
            Random random = new Random();
            Integer randNum = random.nextInt();
            rsp.add("trends",randNum);

            SolrCore.log.warn("In process method, rand value is: " + random);
            System.out.println("In process method, rand value is: " + random);
        }
    }

    /**
     * Called after all responses for a single request were received (after all shards finished the process() method).
     * The method will be invoked for each stage.
     */
    @Override
    public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
        if ((sreq.purpose & TREND_SHARD_REQUEST) != 0) {
            SolrCore.log.info("Aggregator only, handleResponses method is called");

            List<ShardResponse> responses = sreq.responses;

            for (ShardResponse response : responses) {
                // get the 'trends' calc made by the shards (see process())
                Integer shardResult = (Integer)response.getSolrResponse().getResponse().get("trends");

                String msg = "Shard:" +response.getShardAddress() +
                        "response trend is:" + shardResult;
                SolrCore.log.info(msg);
                logger.warn(msg);

                total += shardResult; // the total sum will be written to the response in the finishStage() method.
            }
        }

        // Example of other stage steps
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
            //.....
        }
    }

    /**
     * Will run after the handleResponses() method was done in the aggregator.
     * The method will be invoked for each stage.
     */
    @Override
    public void finishStage(ResponseBuilder rb) {
        if (rb.stage != ResponseBuilder.STAGE_EXECUTE_QUERY) {
            return;
        }

        SolrCore.log.warn("Aggregator only, Add trends_total: " + total);
        rb.rsp.add("trends_total",total);
    }

    //------------------------------------
    // Information suppliers
    //------------------------------------
    @Override
    public NamedList getStatistics() {
        return null;
    }

    @Override
    public String getDescription() {
        return "https://github.com/MrTomerLevi/SolrSearchComponent";
    }

    @Override
    public String getSource() {
        return "SolrSearchComponent";
    }
}
