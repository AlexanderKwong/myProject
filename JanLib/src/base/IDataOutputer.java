package base;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/11/26
 */
public interface IDataOutputer<K, V> {

    void write(String alias, K key, V value)  throws Exception;
}
