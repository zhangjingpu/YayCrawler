package yaycrawler.common.model;

import java.util.List;

/**
 * Created by yuananyun on 2016/8/7.
 */
public class QueueQueryResult {
    private List<CrawlerRequest> rows;
    private long totalPages;
    private long total;

    public List<CrawlerRequest> getRows() {
        return rows;
    }

    public void setRows(List<CrawlerRequest> rows) {
        this.rows = rows;
    }


    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getTotalPages() {
        return totalPages;
    }

    public void setTotalPages(long totalPages) {
        this.totalPages = totalPages;
    }
}
