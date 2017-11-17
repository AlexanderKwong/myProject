package baseDeal;

import base.IModel;
import org.apache.hadoop.util.hash.Hash;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Kwong on 2017/11/10.
 */
public abstract class JoinDeal<O> implements IDeal<IModel, O> {

    public interface ModelMngBuilder<T extends IModel>{

        ModelMngBuilder<T> append(T model);

        ModelMng<T> create();
    }
    public interface ModelMng<T extends IModel>{

        /**
         * 等值连接 inner join TABLE on (arg0 = values[0] and arg1 = values[1] and ...)
         * @param joinCondition 关联条件
         * @return
         */
        T get(JoinCondition joinCondition);

    }

    public interface JoinCondition{}

    public static class HashJoinCondition implements JoinCondition{

        private static final String SEPARATOR = "~!@#";

        private HashJoinCondition(){}

        private String joinKeyHash ;

        public HashJoinCondition generate(Object... values){
            HashJoinCondition joinKey = new HashJoinCondition();
            StringBuilder sb = new StringBuilder();
            for (Object o : values){
                sb.append(o.hashCode()).append(SEPARATOR);
            }
            joinKey.joinKeyHash = sb.toString();
            return joinKey;
        }

        @Override
        public int hashCode() {
            return joinKeyHash.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof HashJoinCondition){
                return joinKeyHash.equals(((HashJoinCondition)obj).joinKeyHash);
            }
            return false;
        }
    }
   /////////////////////////  The following is declaring FIELDS and METHODS  ///////////////////////////////////////


}
