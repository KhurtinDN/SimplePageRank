package ru.sgu.csit.spec.hadoop.pagerank;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: KhurtinDN ( KhurtinDN@gmail.com )
 * Date: 12/16/10
 * Time: 9:02 PM
 */
public class PageRankMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private static final String root = "sgu";
    private static final String server = "http://www.sgu.ru";

    private static final double D = 0.85;
    private static final double PAGE_RANK = 1.0;  // default PageRank = 1.0

    private static final Pattern pattern = Pattern.compile("<a\\s+href\\s*=\\s*[\"']([^\"']*)", Pattern.CASE_INSENSITIVE);

    private Text refText = new Text();
    private DoubleWritable piecePageRank = new DoubleWritable(1.0);

//    private static final GmailChat gmailChat = new GmailChat();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String ref = value.toString().substring(1);

        String page = downloadPage(new Path(root + ref), context);

        List<String> refList = new LinkedList<String>();

        Matcher matcher = pattern.matcher(page);

        while (matcher.find()) {
            String obtainedRef = matcher.group(1);

            obtainedRef = getValidRef(obtainedRef, ref);

            if (obtainedRef != null) {
                refList.add(obtainedRef);
            }
        }

        double piece = refList.isEmpty() ? 0 : D * PAGE_RANK / refList.size();
        piecePageRank.set(piece);

        for (String reference : refList) {
            refText.set(reference);
            context.write(refText, piecePageRank);
        }
    }

    public String downloadPage(Path fileNamePath, Context context) throws IOException {
        byte[] buffer = new byte[1024];
        FileSystem fileSystem = fileNamePath.getFileSystem(context.getConfiguration());

        if (!fileSystem.exists(fileNamePath)) {
            return "";
        }

        FSDataInputStream fsDataInputStream = fileSystem.open(fileNamePath);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        for (int len; (len = fsDataInputStream.read(buffer)) > 0;) {
            byteArrayOutputStream.write(buffer, 0, len);
        }

        String pageContent = new String(byteArrayOutputStream.toByteArray(), Charset.forName("utf8"));

        fsDataInputStream.close();
        byteArrayOutputStream.close();

        return pageContent;
    }

    private String getValidRef(String obtainedRef, String parentRef) {
        if (obtainedRef.contains("..")) {
            return null;
        }

        obtainedRef = obtainedRef.trim().replaceAll(" ", "%20");

        if (obtainedRef.endsWith("?")) {
            obtainedRef = obtainedRef.substring(0, obtainedRef.length() - 1);
        }

        int diesIndex = obtainedRef.indexOf('#');
        if (diesIndex >= 0) {
            obtainedRef = obtainedRef.substring(0, diesIndex);
        }

        if (obtainedRef.equals("/")) {
            return server + "/index.php";
        }

        while (obtainedRef.endsWith("/")) {
            obtainedRef = obtainedRef.substring(0, obtainedRef.length() - 1);
        }

        if (obtainedRef.startsWith("mailto:") || obtainedRef.startsWith("javascript:")) {
            return null;
        } else if (obtainedRef.startsWith("http://")) {
            // ok
        } else if (obtainedRef.startsWith("/")) {
            obtainedRef = server + obtainedRef;
        } else if (obtainedRef.startsWith("?")) {
            int whatIndex = parentRef.indexOf('?');
            if (whatIndex >= 0) {
                obtainedRef = parentRef.substring(0, whatIndex) + obtainedRef;
            } else {
                obtainedRef = parentRef.substring(0, parentRef.lastIndexOf('/') + 1) + obtainedRef;
            }
        } else {
            obtainedRef = parentRef.substring(0, parentRef.lastIndexOf('/') + 1) + obtainedRef;
        }

        if (!obtainedRef.startsWith(server)) {
            return null;
        }

        if (obtainedRef.equals(server)) {
            return server + "/index.php";
        }

        int slashIndex = obtainedRef.lastIndexOf('/');
        if (slashIndex >= 0) {
            String tmp = obtainedRef.substring(slashIndex);
            int dotIndex = tmp.lastIndexOf('.');
            if (dotIndex >= 0) {
                int endIndex = tmp.indexOf("?", dotIndex);
                if (endIndex < 0) {
                    endIndex = tmp.length();
                }
                String ext = tmp.substring(dotIndex + 1, endIndex);
                if (!(ext.equalsIgnoreCase("html") ||
                        ext.equalsIgnoreCase("htm") ||
                        ext.equalsIgnoreCase("php"))) {
                    return null;
                }
            }
        }

        return obtainedRef;
    }
}
