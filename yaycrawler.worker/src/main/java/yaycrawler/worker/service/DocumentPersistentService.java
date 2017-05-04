package yaycrawler.worker.service;

import com.google.common.io.Files;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import us.codecraft.webmagic.utils.UrlUtils;
import yaycrawler.common.utils.FTPUtils;
import yaycrawler.common.utils.FtpClientUtils;
import yaycrawler.common.utils.FtpUtil;
import yaycrawler.common.utils.HttpUtil;
import yaycrawler.spider.persistent.IResultPersistentService;
import yaycrawler.spider.persistent.PersistentDataType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ucs_yuananyun on 2016/5/23.
 */
@Component
public class DocumentPersistentService implements IResultPersistentService {

    @Value("${ftp.server.url}")
    private String url;
    @Value("${ftp.server.port}")
    private int port;
    @Value("${ftp.server.username}")
    private String username;
    @Value("${ftp.server.password}")
    private String password;

    @Value("${ftp.server.path}")
    private String ftpPath;

    private FTPUtils ftpUtil;

    @Override
    /**
     * param data {id:"",srcList:""}
     */
    public boolean saveCrawlerResult(String pageUrl, Map<String, Object> data) {
        try {
            List<String> srcList = new ArrayList<>();
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
                    else if(src instanceof HashedMap) {
                        srcList.add(MapUtils.getString((HashedMap)src,"src"));
                    } else {
                        id = String.valueOf(src);
                    }
                }
                if (srcList == null || srcList.isEmpty())
                    continue;
                else{
                    ftpUtil = new FTPUtils();
                }
                for (String src : srcList) {
                    byte[] bytes = EntityUtils.toByteArray(httpUtil.doGet(src,null,null).getEntity());
                    String documentName = StringUtils.substringAfterLast(src,"/");
                    if (!StringUtils.contains(documentName,".")) {
                        documentName = documentName + ".pdf";
                    }
                    File document = new File(ftpPath + "/" + documentName);
                    Files.createParentDirs(document);
                    Files.write(bytes,document);

                    String path = UrlUtils.getDomain(pageUrl) + "/" + DigestUtils.sha1Hex(pageUrl) + "/" + id;
                    //上传文件
                    ftpUtil.connect(url, port, username, password);
                    ftpUtil.upLoadByFtp(ftpPath + "/" + documentName, path, documentName);
                    ftpUtil.disconnect();
                    document.delete();
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
        return PersistentDataType.DOCMUENT;
    }

}
