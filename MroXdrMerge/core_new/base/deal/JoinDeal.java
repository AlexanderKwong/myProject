package base.deal;

import base.IDeal;
import base.IModel;

import java.util.List;

/**
 * Created by Kwong on 2017/11/10.
 * ps. 入参声明为IModel而不是某个特定的泛型，为的是保留一些灵活性，让开发者选择在一个deal中关联多种数据 还是 声明多个deal来关联多种数据
 * pss. 返回值也该声明为IModel吧，来的数据如果不是这个deal能处理的，应该往下传递
 */
public abstract class JoinDeal<O extends IModel> implements IDeal<IModel, O> {

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

        public static HashJoinCondition generate(Object... values){
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
