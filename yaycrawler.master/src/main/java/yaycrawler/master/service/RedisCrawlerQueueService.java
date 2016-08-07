package yaycrawler.master.service;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;
import yaycrawler.common.model.CrawlerRequest;
import yaycrawler.common.model.QueueQueryParam;
import yaycrawler.common.model.QueueQueryResult;

import java.util.*;

/**
 * Created by yuananyun on 2016/8/7.
 */
@Component
public class RedisCrawlerQueueService implements ICrawlerQueueService {

    private static final Logger logger = LoggerFactory.getLogger(RedisCrawlerQueueService.class);
    @Autowired
    private RedisTemplate redisTemplate;
    private int batchSize = 1000;

    private static final String DUPLICATE_REMOVAL_PREFIX = "history:set_";
    private static final String TASK_DETAIL_HASH_PREFIX = "detail:hash_";
    private static final String WAITING_QUEUE_PREFIX = "waiting:queue_";
    private static final String RUNNING_QUEUE_PREFIX = "running:queue_";
    private static final String RUNNING_EXTRA_PREFIX = "running_extra:hash_";
    private static final String FAIL_QUEUE_PREFIX = "fail:queue_";
    private static final String FAIL_EXTRA_PREFIX = "fail_extra:hash_";
    private static final String SUCCESS_QUEUE_PREFIX = "success:queue_";


    /**
     * 防止任务重复的key集合
     *
     * @return
     */
    private String getUniqueKeySetIdentification() {
        return DUPLICATE_REMOVAL_PREFIX + "data";
    }

    /**
     * 获取任务详细信息的hash表标识
     *
     * @return
     */
    private String getTaskInfoHashIdentification() {
        return TASK_DETAIL_HASH_PREFIX + "data";
    }

    private String getWaitingQueueIdentification() {
        return WAITING_QUEUE_PREFIX + "data";
    }

    private String getRunningQueueIdentification() {
        return RUNNING_QUEUE_PREFIX + "data";
    }

    private String getRunningQueueExtraInfoIdentification() {
        return RUNNING_EXTRA_PREFIX + "data";
    }

    private String getFailQueueIdentification() {
        return FAIL_QUEUE_PREFIX + "data";
    }

    private String getFailQueueExtraInfoIdentification() {
        return FAIL_EXTRA_PREFIX + "data";
    }

    private String getSuccessQueueIdentification() {
        return SUCCESS_QUEUE_PREFIX + "data";
    }


    /**
     * 添加任务到待执行队列
     *
     * @param crawlerRequests  任务
     * @param removeDuplicated 当存在相同任务时是否移除相同任务
     * @return
     */
    @Override
    public boolean pushTasksToWaitingQueue(List<CrawlerRequest> crawlerRequests, boolean removeDuplicated) {
        try {
            logger.info("开始注册{}个任务", crawlerRequests.size());
            Map<String, String> taskDetailsMap = Maps.newHashMap();

            for (CrawlerRequest request : crawlerRequests) {
                if (!removeDuplicated && isDuplicate(request)) continue;

                String url = getUniqueUrl(request);
                request.setUrl(url);
                String hashCode = DigestUtils.sha1Hex(url);
                request.setHashCode(hashCode);
                taskDetailsMap.put(hashCode, JSON.toJSONString(request));

                if (taskDetailsMap.size() >= batchSize) {
                    batchPushToWaitingQueue(taskDetailsMap);
                    taskDetailsMap.clear();
                }
            }
            batchPushToWaitingQueue(taskDetailsMap);
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }

    }

    /**
     * 从待执行队列中拿取指定数目的任务
     *
     * @param taskCount 任务数目
     * @return
     */
    @Override
    public List<CrawlerRequest> fetchTasksFromWaitingQueue(long taskCount) {
        HashSet<String> taskCodeList = (HashSet<String>) redisTemplate.opsForZSet().range(getWaitingQueueIdentification(), 0, taskCount - 1);
        return getCrawlerRequestsByCodes(taskCodeList);
    }

    /**
     * 将任务从待执行移到执行中队列
     *
     * @param workerId        任务在哪个Worker上执行
     * @param crawlerRequests
     */
    @Override
    public boolean moveWaitingTaskToRunningQueue(String workerId, List<CrawlerRequest> crawlerRequests) {
        List<String> taskCodeList = new ArrayList<>(crawlerRequests.size());
        ZSetOperations zSetOperations = redisTemplate.opsForZSet();
        for (CrawlerRequest crawlerRequest : crawlerRequests) {
            String hashCode = crawlerRequest.getHashCode();
            taskCodeList.add(hashCode);
            zSetOperations.add(getRunningQueueIdentification(), hashCode, System.currentTimeMillis());
            zSetOperations.remove(getWaitingQueueIdentification(), hashCode);
        }
        //保存任务所在的workerId？
        return true;
    }

    /**
     * 当任务执行失败后将任务移到失败队列
     *
     * @param taskCode
     * @param message
     * @return
     */
    @Override
    public boolean moveRunningTaskToFailQueue(String taskCode, String message) {
        if (redisTemplate.opsForZSet().add(getFailQueueIdentification(), taskCode, System.currentTimeMillis())) {
            redisTemplate.opsForHash().put(getFailQueueExtraInfoIdentification(), taskCode, message);
        }
        redisTemplate.opsForZSet().remove(getRunningQueueIdentification(), taskCode);
        //清理去重列表
        redisTemplate.opsForSet().remove(getUniqueKeySetIdentification(), taskCode);
        return true;
    }

    /**
     * 从执行中队列把成功的任务移到成功队列
     *
     * @param taskCode
     * @return
     */
    @Override
    public boolean moveRunningTaskToSuccessQueue(String taskCode) {
        if (redisTemplate.opsForZSet().add(getSuccessQueueIdentification(), taskCode, System.currentTimeMillis())) {
            redisTemplate.opsForZSet().remove(getRunningQueueIdentification(), taskCode);
            //清理去重列表
            redisTemplate.opsForSet().remove(getUniqueKeySetIdentification(), taskCode);
            return true;
        }
        return false;
    }

    /**
     * 刷新超时队列（把超时的运行中队列任务重新加入待执行队列）
     *
     * @param timeout
     */
    @Override
    public void refreshBreakedQueue(Long timeout) {
        Long endTime = System.currentTimeMillis() - timeout;
        Set<String> timeoutTaskCodeSet = (Set<String>) redisTemplate.opsForZSet().rangeByScore(getRunningQueueIdentification(), 0, endTime);
        for (String code : timeoutTaskCodeSet) {
            //加入待执行队列
            redisTemplate.opsForZSet().add(getWaitingQueueIdentification(), code, System.currentTimeMillis());
            //加入任务去重集合
            redisTemplate.opsForSet().add(getUniqueKeySetIdentification(), code);
        }
        redisTemplate.opsForZSet().removeRangeByScore(getRunningQueueIdentification(), 0, endTime);
    }

    /**
     * 查询待执行队列
     *
     * @param queryParam
     * @return
     */
    @Override
    public QueueQueryResult queryWaitingQueues(QueueQueryParam queryParam) {
        return queueQueryByParam(queryParam);
    }


    /**
     * 查询执行中队列
     *
     * @param queryParam
     * @return
     */
    @Override
    public QueueQueryResult queryRunningQueues(QueueQueryParam queryParam) {
        return queueQueryByParam(queryParam);
    }

    /**
     * 查询失败队列
     *
     * @param queryParam
     * @return
     */
    @Override
    public QueueQueryResult queryFailQueues(QueueQueryParam queryParam) {
        return queueQueryByParam(queryParam);
    }

    /**
     * 查询成功队列
     *
     * @param queryParam
     * @return
     */
    @Override
    public QueueQueryResult querySuccessQueues(QueueQueryParam queryParam) {
        return queueQueryByParam(queryParam);
    }

/******************************************************************私有方法*********************************************************/
    /**
     * 为爬虫任务生成一个唯一的url
     *
     * @param crawlerRequest 请求
     * @return
     */
    private String getUniqueUrl(CrawlerRequest crawlerRequest) {
        if (crawlerRequest.getData() == null || crawlerRequest.getData().size() == 0)
            return crawlerRequest.getUrl();
        StringBuilder urlBuilder = new StringBuilder(crawlerRequest.getUrl().trim());
        String random = DigestUtils.sha1Hex(JSON.toJSONString(crawlerRequest.getData()));
        urlBuilder.append(String.format("%s%s=%s", urlBuilder.indexOf("?") > 0 ? "&" : "?", "random", random));
        return urlBuilder.toString();
    }

    /**
     * 批量加入任务到等待队列
     *
     * @param taskDetailsMap
     * @return
     */
    private boolean batchPushToWaitingQueue(Map<String, String> taskDetailsMap) {
        try {
            if (taskDetailsMap == null || taskDetailsMap.size() == 0) return false;
            for (Map.Entry<String, String> entry : taskDetailsMap.entrySet()) {
                //加入待执行队列
                redisTemplate.opsForZSet().add(getWaitingQueueIdentification(), entry.getKey(), System.currentTimeMillis());
                //加入任务信息表
                redisTemplate.opsForHash().put(getTaskInfoHashIdentification(), entry.getKey(), entry.getValue());
                //加入任务去重集合
                redisTemplate.opsForSet().add(getUniqueKeySetIdentification(), entry.getKey());
            }
            return true;
        } catch (Exception e) {
            logger.info("任务{}添加失败！错误：{}", JSON.toJSONString(taskDetailsMap.keySet()), e.getMessage());
            return false;
        }
    }

    /**
     * 判断一个爬虫任务是否重复
     *
     * @param request
     * @return
     */
    private boolean isDuplicate(CrawlerRequest request) {
        return redisTemplate.opsForSet().isMember(getUniqueKeySetIdentification(), DigestUtils.sha1Hex(getUniqueUrl(request)));
    }

    /**
     * 根据Code获取任务信息
     *
     * @param taskCodeList
     * @return
     */
    private List<CrawlerRequest> getCrawlerRequestsByCodes(Set<String> taskCodeList) {
        List<String> taskDetailList = redisTemplate.opsForHash().multiGet(getTaskInfoHashIdentification(), taskCodeList);
        List<CrawlerRequest> taskList = new ArrayList<>(taskDetailList.size());
        for (String detail : taskDetailList) {
            CrawlerRequest crawlerRequest = JSON.parseObject(detail, CrawlerRequest.class);
            taskList.add(crawlerRequest);
        }
        return taskList;
    }

    /**
     * 根据参数查询队列结果
     *
     * @param queryParam
     * @return
     */
    private QueueQueryResult queueQueryByParam(QueueQueryParam queryParam) {
        int pageIndex = queryParam.getPageIndex();
        int pageSize = queryParam.getPageSize();
        String queueName = queryParam.getName();

        String queueIdentification = "";
        String extraDataIdentification = null;
        if (StringUtils.equalsIgnoreCase(queueName, "waiting")) {
            queueIdentification = getWaitingQueueIdentification();
        } else if (StringUtils.equalsIgnoreCase(queueName, "running")) {
            queueIdentification = getRunningQueueIdentification();
        } else if (StringUtils.equalsIgnoreCase(queueName, "success")) {
            queueIdentification = getSuccessQueueIdentification();
        } else if (StringUtils.equalsIgnoreCase(queueName, "fail")) {
            queueIdentification = getFailQueueIdentification();
            extraDataIdentification = getFailQueueExtraInfoIdentification();
        }
        long total = redisTemplate.opsForZSet().size(queueIdentification);
        long pageCount = total / pageSize + (total % pageSize == 0 ? 0 : 1);
        QueueQueryResult queryResult = new QueueQueryResult();
        queryResult.setTotal(total);
        queryResult.setTotalPages(pageCount);

        List<CrawlerRequest> crawlerRequestList = new ArrayList<>();
        Set<DefaultTypedTuple> tupleList = redisTemplate.opsForZSet().rangeWithScores(queueIdentification, (pageIndex) * pageSize, (pageIndex + 1) * pageSize - 1);
        for (DefaultTypedTuple tuple : tupleList) {
            String code = String.valueOf(tuple.getValue());
            String detail = String.valueOf(redisTemplate.opsForHash().get(getTaskInfoHashIdentification(), code));
            CrawlerRequest crawlerRequest = JSON.parseObject(detail, CrawlerRequest.class);
            if (crawlerRequest == null) continue;
            if (crawlerRequest.getExtendMap() == null) crawlerRequest.setExtendMap(new HashedMap());
            crawlerRequest.getExtendMap().put("startTime", tuple.getScore().longValue());
            if (crawlerRequest != null) {
                if (extraDataIdentification != null) {
                    Object extraInfo = redisTemplate.opsForHash().get(extraDataIdentification, code);
                    crawlerRequest.getExtendMap().put("extraInfo", extraInfo);
                }
                crawlerRequestList.add(crawlerRequest);
            }

        }
        queryResult.setRows(crawlerRequestList);
        return queryResult;
    }
}
