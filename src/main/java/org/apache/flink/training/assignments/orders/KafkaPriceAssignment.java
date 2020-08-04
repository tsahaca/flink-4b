package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class KafkaPriceAssignment extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPriceAssignment.class);


    /**
     // --KAFKA_ADDRESS kafka.dest.tanmay.wsn.riskfocus.com:9092 --IN_PRICE price --IN_PACT positionsByAct --IN_PSYMBOL positionsBySymbol --OUT_MV_ACT mvByAct --OUT_MV_SYMBOL mvBySymbol
     */
    public static void main(String[] args) throws Exception {

        final String KAFKA_ADDRESS;
        final String KAFKA_GROUP;
        final String IN_PRICE;
        final String IN_POSITION_ACT;
        final String IN_POSITION_SYMBOL;
        final String OUT_MV_SYMBOL;
        final String OUT_MV_ACT;

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            KAFKA_ADDRESS = params.getRequired("KAFKA_ADDRESS");
            KAFKA_GROUP = params.has("KAFKA_GROUP") ? params.get("KAFKA_GROUP") : "";

            IN_PRICE = params.has("IN_PRICE") ? params.get("IN_PRICE") : "price";
            IN_POSITION_ACT = params.has("IN_PACT") ? params.get("IN_PACT") : "positionsByAct";
            IN_POSITION_SYMBOL = params.has("IN_PSYMBOL") ? params.get("IN_PSYMBOL") : "positionsBySymbol";

            OUT_MV_ACT = params.has("OUT_MV_ACT") ? params.get("OUT_MV_ACT") : "mvByAct";
            OUT_MV_SYMBOL = params.has("OUT_MV_SYMBOL") ? params.get("OUT_MV_SYMBOL") : "mvBySymbol";


        } catch (Exception e) {
            System.err.println("No KAFKA_ADDRESS specified. Please run 'KafkaOrderAssignment \n" +
                    "--KAFKA_ADDRESS <localhost:9092> --IN_PRICE <price> --OUT_TOPIC <demo-output>', \n" +
                    "where KAFKA_ADDRESS is bootstrap-server and \n" +
                    "IN_TOPIC is order input topic and \n" +
                    "OUT_TOPIC is position output topic");
            return;
        }
        final Map<String,String> params = new HashMap<String, String>();
        params.put(IConstants.KAFKA_ADDRESS, KAFKA_ADDRESS);
        params.put(IConstants.KAFKA_GROUP, KAFKA_GROUP);

        params.put(IConstants.IN_PRICE, IN_PRICE);
        params.put(IConstants.IN_POSITION_ACT, IN_POSITION_ACT);
        params.put(IConstants.IN_POSITION_SYMBOL, IN_POSITION_SYMBOL);

        params.put(IConstants.OUT_MV_ACT, OUT_MV_ACT);
        params.put(IConstants.OUT_MV_SYMBOL, OUT_MV_SYMBOL);

        final OrderPipeline pipeline = new OrderPipeline(params);
        pipeline.execute();
    }

}
