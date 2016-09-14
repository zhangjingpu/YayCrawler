package yaycrawler.worker.service;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import us.codecraft.webmagic.utils.UrlUtils;
import yaycrawler.common.utils.FtpClientUtils;
import yaycrawler.common.utils.HttpUtil;
import yaycrawler.spider.persistent.IResultPersistentService;
import yaycrawler.spider.persistent.PersistentDataType;

import java.util.List;
import java.util.Map;

/**
 * Created by ucs_yuananyun on 2016/5/23.
 */
@Component
public class MusicPersistentService implements IResultPersistentService {

    @Value("${ftp.server.url}")
    private String url;
    @Value("${ftp.server.port}")
    private int port;
    @Value("${ftp.server.username}")
    private String username;
    @Value("${ftp.server.password}")
    private String password;

    @Override
    /**
     * param data {id:"",srcList:""}
     */
    public boolean saveCrawlerResult(String pageUrl, Map<String, Object> data) {
        //TODO 下载音乐
        try {
            List<String> srcList = null;
            String id = "";
            HttpUtil httpUtil = HttpUtil.getInstance();
//            List<Header> headers = new ArrayList<>();
//            headers.add(new BasicHeader("",""));
            for (Object o : data.values()) {
                Map<String,Object> regionData=(Map<String,Object>)o;
                if(regionData==null) continue;
                for (Object src : regionData.values()) {
                    if (src instanceof List)
                        srcList = (List<String>) src;
                    else
                        id = String.valueOf(src);
                }
                if (srcList == null || srcList.isEmpty())
                    continue;
                for (String src : srcList) {
//                    byte[] bytes = EntityUtils.toByteArray(httpUtil.doGet(src,null,headers).getEntity());
//                    String imgName = StringUtils.substringAfterLast(src,"/");
//                    if (!StringUtils.contains(imgName,".")) {
//                        imgName = imgName + ".jpg";
//                    }
//                    File img = new File(imagePath + "/" + id +  "/" + imgName);
//                    Files.createParentDirs(img);
//                    Files.write(bytes,img);
                    String musicName = StringUtils.substringAfterLast(src,"/");
                    if (!StringUtils.contains(musicName,".")) {
                        musicName = musicName + ".MP3";
                    }
                    String path = UrlUtils.getDomain(pageUrl) + "/" + DigestUtils.sha1Hex(pageUrl) + "/" + id;
                    FtpClientUtils.uploadFile(url,port,username,password,path,musicName,httpUtil.doGetForStream(src,null));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public String getSupportedDataType() {
        return PersistentDataType.MUSIC;
    }

}
