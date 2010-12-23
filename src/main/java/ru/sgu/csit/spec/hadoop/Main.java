package ru.sgu.csit.spec.hadoop;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.sgu.csit.spec.hadoop.pagerank.PageRankTool;

/**
 * User: KhurtinDN ( KhurtinDN@gmail.com )
 * Date: 12/16/10
 * Time: 3:09 PM
 */
public class Main {
    public static void main(String[] args) throws Exception {
//        Tool tool = new WordCountTool();
        Tool tool = new PageRankTool();
        int result = ToolRunner.run(tool, args);

        System.exit(result);
    }
}
