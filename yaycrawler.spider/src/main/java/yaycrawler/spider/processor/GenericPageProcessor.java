package yaycrawler.spider.processor;

import com.github.stuxuhai.jpinyin.PinyinException;
import com.github.stuxuhai.jpinyin.PinyinFormat;
import com.github.stuxuhai.jpinyin.PinyinHelper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Json;
import us.codecraft.webmagic.selector.Selectable;
import yaycrawler.common.model.CrawlerRequest;
import yaycrawler.dao.domain.*;
import yaycrawler.dao.service.PageParserRuleService;
import yaycrawler.monitor.captcha.CaptchaIdentificationProxy;
import yaycrawler.monitor.login.AutoLoginProxy;
import yaycrawler.spider.listener.IPageParseListener;
import yaycrawler.spider.resolver.SelectorExpressionResolver;
import yaycrawler.spider.service.PageSiteService;

import java.util.*;

/**
 * Created by yuananyun on 2016/5/1.
 */
@Component(value = "genericPageProcessor")
public class GenericPageProcessor implements PageProcessor {
    private static Logger logger = LoggerFactory.getLogger(GenericPageProcessor.class);
    private static String DEFAULT_PAGE_SELECTOR = "page";

    @Autowired(required = false)
    private IPageParseListener pageParseListener;
    @Autowired
    private PageParserRuleService pageParserRuleService;
    @Autowired
    private PageSiteService pageSiteService;
    @Autowired
    private AutoLoginProxy autoLoginProxy;
    @Autowired
    private CaptchaIdentificationProxy captchaIdentificationProxy;


    @Override
    public void process(Page page) {
        Request pageRequest = page.getRequest();
        String pageUrl = pageRequest.getUrl();
        if (doAutomaticRecovery(page, pageRequest, pageUrl)) {
//            //重新加入队列
//            page.addTargetRequest(page.getRequest());
            return;
        }
        //是否正确的页面
        PageInfo pageInfo = pageParserRuleService.findOnePageInfoByRgx(pageUrl);
        if(pageInfo==null) return;
        String pageValidationExpression = pageInfo.getPageValidationRule();
        if (pageValidated(page, pageValidationExpression)) {
            try {
                List<CrawlerRequest> childRequestList = new LinkedList<>();
                Set<PageParseRegion> regionList = pageParserRuleService.getPageRegions(pageUrl);
                for (PageParseRegion pageParseRegion : regionList) {
                    Map<String, Object> result = parseOneRegion(page, pageParseRegion, childRequestList);
                    if (result != null) {
                        result.put("dataType", pageParseRegion.getDataType());
                        page.putField(pageParseRegion.getName(), result);
                    }
                }
                if (pageParseListener != null)
                    pageParseListener.onSuccess(pageRequest, childRequestList);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
                if (pageParseListener != null)
                    pageParseListener.onError(pageRequest, "页面解析失败");
            }
        } else {
            //页面下载错误，验证码或cookie失效
            if (pageParseListener != null)
                pageParseListener.onError(pageRequest, "下载的页面不是我想要的");
        }
    }


    @SuppressWarnings("all")
    public Map<String, Object> parseOneRegion(Page page, PageParseRegion pageParseRegion, List<CrawlerRequest> childRequestList) {
        Request request = page.getRequest();
        String selectExpression = pageParseRegion.getSelectExpression();

        Selectable context = getPageRegionContext(page, request, selectExpression);
        if (context == null) return null;

        Set<UrlParseRule> urlParseRules = pageParseRegion.getUrlParseRules();
        if (urlParseRules != null && urlParseRules.size() > 0) {
            childRequestList.addAll(parseUrlRules(context, request, urlParseRules));
        }

        Set<FieldParseRule> fieldParseRules = pageParseRegion.getFieldParseRules();
        if (fieldParseRules != null && fieldParseRules.size() > 0) {
            return parseFieldRules(context, request, fieldParseRules,pageParseRegion.getDataType());
        }

        return null;
    }

    /**
     * 获取一个region的上下文
     *
     * @param page
     * @param request
     * @param regionSelectExpression
     * @return
     */
    private Selectable getPageRegionContext(Page page, Request request, String regionSelectExpression) {
        Selectable context;
        if (StringUtils.isBlank(regionSelectExpression) || DEFAULT_PAGE_SELECTOR.equals(regionSelectExpression))
            context = page.getHtml();
        else if (regionSelectExpression.toLowerCase().contains("getjson()") || regionSelectExpression.toLowerCase().contains("jsonpath"))
            context = SelectorExpressionResolver.resolve(request, page.getJson(), regionSelectExpression);
        else
            context = SelectorExpressionResolver.resolve(request, page.getHtml(), regionSelectExpression);
        return context;
    }

    /**
     * 解析一个字段抽取规则
     *
     * @param context
     * @param request
     * @param fieldParseRuleList
     * @return
     */
    private Map<String, Object> parseFieldRules(Selectable context, Request request, Collection<FieldParseRule> fieldParseRuleList,String dataType) {
        int i = 0;
        HashedMap resultMap = new HashedMap();
        List<Selectable> nodes = getNodes(context);

        for (Selectable node : nodes) {
            HashedMap childMap = new HashedMap();
            for (FieldParseRule fieldParseRule : fieldParseRuleList) {
                Object datas = childMap.get(fieldParseRule.getFieldName());
                if(datas == null) {
                    datas = SelectorExpressionResolver.resolve(request, node, fieldParseRule.getRule());
                } else {
                    List tmp = new ArrayList();
                    tmp.add(datas);
                    tmp.add(SelectorExpressionResolver.resolve(request, node, fieldParseRule.getRule()));
                    datas = tmp;
                }
                if((datas == null && "label".equalsIgnoreCase(fieldParseRule.getFieldName()))||(datas == null && childMap.get("label") == null && "value".equalsIgnoreCase(fieldParseRule.getFieldName()))||(fieldParseRuleList.size() == 1 && datas.toString() == null && !"label".equalsIgnoreCase(fieldParseRule.getFieldName()) && ! "value".equalsIgnoreCase(fieldParseRule.getFieldName())))
                    continue;
                childMap.put(fieldParseRule.getFieldName(), datas);
            }
            if(StringUtils.equalsIgnoreCase(dataType,"autoField") && MapUtils.getString(childMap,"label") != null && MapUtils.getString(childMap,"value") != null) {
                try {
                    HashedMap dataMap = null;
                    if(MapUtils.getObject(childMap,"value") instanceof Collection) {
                        Object labels = MapUtils.getObject(childMap,"label");
                        List<String> values = (ArrayList)MapUtils.getObject(childMap,"value");
                        for (int j = 0; j < values.size(); j++) {
                            dataMap = (HashedMap) resultMap.get(String.valueOf(j));
                            if(dataMap == null) {
                                dataMap = new HashedMap();
                            }
                            if(MapUtils.getObject(childMap,"label") instanceof Collection) {
                                dataMap = (HashedMap) resultMap.get(String.valueOf(0));
                                if(dataMap == null) {
                                    dataMap = new HashedMap();
                                }
                                if(((List)labels).get(j) != null) {
                                    dataMap.put(PinyinHelper.convertToPinyinString(((List) labels).get(j).toString(), "", PinyinFormat.WITHOUT_TONE), values.get(j));
                                    resultMap.put(String.valueOf(0), dataMap);
                                }
                            } else {
                                dataMap.put(PinyinHelper.convertToPinyinString(MapUtils.getString(childMap,"label"),"", PinyinFormat.WITHOUT_TONE),values.get(j));
                                resultMap.put(String.valueOf(j),dataMap);
                            }
                            if(MapUtils.getObject(childMap,"label") instanceof Collection) {
                                dataMap = (HashedMap) resultMap.get(String.valueOf(0));
                                if(dataMap == null) {
                                    dataMap = new HashedMap();
                                }
                                if(((List)labels).get(j) != null) {
                                    dataMap.put(PinyinHelper.convertToPinyinString(((List) labels).get(j).toString(), "", PinyinFormat.WITHOUT_TONE), values.get(j));
                                    resultMap.put(String.valueOf(0), dataMap);
                                }
                            } else {
                                dataMap.put(PinyinHelper.convertToPinyinString(MapUtils.getString(childMap,"label"),"", PinyinFormat.WITHOUT_TONE),values.get(j));
                                resultMap.put(String.valueOf(j),dataMap);
                            }
                        }
                    } else {
                        resultMap.put(PinyinHelper.convertToPinyinString(MapUtils.getString(childMap,"label"),"", PinyinFormat.WITHOUT_TONE),MapUtils.getString(childMap,"value"));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if(StringUtils.equalsIgnoreCase(dataType,"autoRowField") && MapUtils.getObject(childMap,"label") != null && MapUtils.getString(childMap,"value") != null) {
                List labels = (ArrayList)MapUtils.getObject(childMap,"label");
                List<List> values = (ArrayList)MapUtils.getObject(childMap,"value");
                int k = 0;
                for (List value:values) {
                    HashedMap dataMap = new HashedMap();
                    for (int j = 0; j < labels.size(); j++) {
                        try {
                            dataMap.put(PinyinHelper.convertToPinyinString(labels.get(j).toString(),"",PinyinFormat.WITHOUT_TONE),value.get(j));
                        } catch (PinyinException e) {
                            e.printStackTrace();
                        }
                    }
                    resultMap.put(String.valueOf(k++),dataMap);
                }
            }else if(StringUtils.equalsIgnoreCase(dataType,"autoRowField") && MapUtils.getObject(childMap,"value") instanceof Collection){
                Collection childs = (Collection)MapUtils.getObject(childMap,"value");
                int j = 0;
                for (Object child : childs) {
                    resultMap.put(String.valueOf(j++), child);
                }
            } else if((StringUtils.equalsIgnoreCase(dataType,"autoField") || StringUtils.equalsIgnoreCase(dataType,"autoRowField"))&& MapUtils.getString(childMap,"label") == null &&  MapUtils.getString(childMap,"value") == null) {
                resultMap.putAll(childMap);
            }else {
                resultMap.put(String.valueOf(i++), childMap);
            }
            if((StringUtils.equalsIgnoreCase(dataType,"autoField") || StringUtils.equalsIgnoreCase(dataType,"autoRowField")) && (MapUtils.getString(childMap,"label") != null &&  MapUtils.getString(childMap,"value") != null)){
                for (Object o : childMap.entrySet()) {
                    Map.Entry<String,Object> item = (Map.Entry<String, Object>) o;
                    if(!(StringUtils.equalsIgnoreCase(item.getKey(),"label") || StringUtils.equalsIgnoreCase(item.getKey(),"value"))){
                        for (Object o1 : resultMap.values()) {
                            if (o1 instanceof HashedMap) {
                                HashedMap dataMap = (HashedMap) o1;
                                try {
                                    dataMap.put(PinyinHelper.convertToPinyinString(item.getKey().toLowerCase().toString(), "", PinyinFormat.WITHOUT_TONE), item.getValue());
                                } catch (PinyinException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
            }

        }
        if (nodes.size() > 1 ||StringUtils.equalsIgnoreCase(dataType,"autoField") ||StringUtils.equalsIgnoreCase(dataType,"autoRowField"))
            return resultMap;
        else
            return (Map<String, Object>) resultMap.get("0");
    }


    /**
     * 解析一个Url抽取规则
     *
     * @param context
     * @param request
     * @param urlParseRuleList
     * @return
     */
    private List<CrawlerRequest> parseUrlRules(Selectable context, Request request, Collection<UrlParseRule> urlParseRuleList) {
        List<CrawlerRequest> childRequestList = new LinkedList<>();
        List<Selectable> nodes = getNodes(context);

        for (Selectable node : nodes) {
            if (node == null) continue;

            for (UrlParseRule urlParseRule : urlParseRuleList) {
                //解析url
                Object u = SelectorExpressionResolver.resolve(request, node, urlParseRule.getRule());
                //解析Url的参数
                Map<String, Object> urlParamMap = new HashMap<>();
                if (urlParseRule.getUrlRuleParams() != null)
                    for (UrlRuleParam ruleParam : urlParseRule.getUrlRuleParams()) {
                        urlParamMap.put(ruleParam.getParamName(), SelectorExpressionResolver.resolve(request, node, ruleParam.getExpression()));
                    }
                //组装成完整的URL
                if (u instanceof Collection) {
                    Collection<String> urlList = (Collection<String>) u;
                    if (urlList.size() > 0)
                        for (String url : urlList)
                            childRequestList.add(new CrawlerRequest(url, urlParseRule.getMethod(), urlParamMap));
                } else
                    childRequestList.add(new CrawlerRequest((String) u, urlParseRule.getMethod(), urlParamMap));
            }
        }
        return childRequestList;
    }


    private List<Selectable> getNodes(Selectable context) {
        List<Selectable> nodes = new LinkedList<>();

        if (context instanceof Json) {
            nodes.add(context);
        } else nodes.addAll(context.nodes());
        return nodes;
    }


    @Override
    public Site getSite() {
        return Site.me();
    }

    /**
     * 页面自动恢复
     *
     * @param page
     * @param pageRequest
     * @param pageUrl
     */
    private boolean doAutomaticRecovery(Page page, Request pageRequest, String pageUrl) {
        boolean doRecovery = false;
        PageSite pageSite = pageSiteService.getPageSiteByUrl(pageUrl);
        if (pageSite != null) {
            String loginJudgeExpression = pageSite.getLoginJudgeExpression();
            String captchaJudgeExpression = pageSite.getCaptchaJudgeExpression();
            String loginJsFileName = pageSite.getLoginJsFileName();
            String captchaJsFileName = pageSite.getCaptchaJsFileName();
            String oldCookieId = (String) pageRequest.getExtra("cookieId");

            Selectable judgeContext = StringUtils.isNotBlank(loginJsFileName) ? getPageRegionContext(page, pageRequest, loginJudgeExpression) : null;
            if (judgeContext != null && judgeContext.match()) {
                doRecovery = true;
                //需要登录了
                autoLoginProxy.login(pageUrl, loginJsFileName, page.getRawText(), oldCookieId);
                //重新加入队列
                page.addTargetRequest(pageRequest);
            } else {
                judgeContext = StringUtils.isNotBlank(captchaJsFileName) ? getPageRegionContext(page, pageRequest, captchaJudgeExpression) : null;
                if (judgeContext != null && judgeContext.match()) {
                    doRecovery = true;
                    //需要刷新验证码了
                    captchaIdentificationProxy.recognition(pageUrl, captchaJsFileName, page.getRawText(), oldCookieId);
                }
            }
        }
        return doRecovery;
    }


    /**
     * 验证是否正确的页面
     *
     * @param page
     * @param pageValidationExpression
     * @return
     */
    public boolean pageValidated(Page page, String pageValidationExpression) {
        if (StringUtils.isEmpty(pageValidationExpression)) return true;
        Request request = page.getRequest();
        Object result = getPageRegionContext(page, request, pageValidationExpression);
        if (result == null) return false;
        if (result instanceof Selectable)
            return ((Selectable) result).match();
        else
            return StringUtils.isNotEmpty(String.valueOf(result));

    }
}
