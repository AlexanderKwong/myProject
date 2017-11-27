package base;

/**
 * Created by Kwong on 2017/11/27.
 */
public interface Cacheable {
    /**
     * 获取缓存的 阈值，超过此值就得 flushAllCache()
     * @return
     */
    int flushThreshold();

    /**
     * 清理所有的缓存
     */
    void flushAllCache();
}
