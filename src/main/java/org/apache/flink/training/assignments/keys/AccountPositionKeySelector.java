package org.apache.flink.training.assignments.keys;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.training.assignments.domain.Position;

/**
 * This is the key to group position by account, sub-account and cusip
 */
public class AccountPositionKeySelector implements KeySelector<Position, Tuple3<String, String, String>> {

    @Override
    public Tuple3<String, String, String> getKey(final Position position) throws Exception
    {
        return new Tuple3<String, String, String>(position.getAccount(),
                position.getSubAccount(), position.getCusip());
    }
}
