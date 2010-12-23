package ru.sgu.csit.spec.hadoop.downloader;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: KhurtinDN ( KhurtinDN@gmail.com )
 * Date: 12/20/10
 * Time: 9:14 AM
 */
public class DownloadManager {
    private static final Logger logger = Logger.getLogger(DownloadManager.class);
    private static final Logger downloadedFilesLogger = Logger.getLogger("downloaded-files");

    private static final Pattern pattern = Pattern.compile("<a\\s+href\\s*=\\s*[\"']([^\"']*)", Pattern.CASE_INSENSITIVE);

    private String root;
    private String destination;
    private String server;

    byte[] buffer = new byte[1024];

    public DownloadManager(String startPage, String destination) throws MalformedURLException, FileNotFoundException {
        this.root = startPage;
        this.destination = destination;

        URL url = new URL(startPage);
        this.server = url.getProtocol() + "://" + url.getHost();// + ":" + url.getPort();
    }

    public String downloadPage(URL pageUrl) throws IOException {
        InputStream inputStream = pageUrl.openStream();

        String result;
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

            for (int len; (len = inputStream.read(buffer)) > 0;) {
                byteArrayOutputStream.write(buffer, 0, len);
            }

            result = new String(byteArrayOutputStream.toByteArray(), Charset.forName("utf8"));

            byteArrayOutputStream.close();
        } finally {
            inputStream.close();
        }

        return result;
    }

    public void savePageToFile(URL pageUrl, String pageContent) throws FileNotFoundException {
        String fileName = destination + pageUrl.getFile() + ".saved.html";
        File destinationDirectory = new File(fileName.substring(0, fileName.lastIndexOf("/")));
        if (!destinationDirectory.exists() && !destinationDirectory.mkdirs()) {
            throw new RuntimeException("mkdir '" + destinationDirectory.getAbsolutePath() + "' failed!");
        }

        PrintWriter printWriter = new PrintWriter(fileName);
        printWriter.println(pageContent);
        printWriter.close();

        downloadedFilesLogger.info(fileName);
    }

    public boolean download() throws FileNotFoundException {
        Queue<String> needVisitPageSet = new LinkedList<String>();
        Set<String> visitedPageSet = new HashSet<String>();

        String ref = getValidRef(root, root);

        needVisitPageSet.add(ref);
        visitedPageSet.add(ref);


        while (!needVisitPageSet.isEmpty()) {
            ref = needVisitPageSet.remove();

            logger.info("[DOWNLOAD] (" + needVisitPageSet.size() + "/" + visitedPageSet.size() + "): " + ref);
            String page;
            try {
                URL pageUrl = new URL(ref);
                page = downloadPage(pageUrl);
                if (page.length() < 10) {
                    throw new IOException("Small content");
                }
                savePageToFile(pageUrl, page);
            } catch (IOException e) {
                logger.debug("[SKIP] (ERROR during download): " + ref);
                continue;
            }

            Matcher matcher = pattern.matcher(page);

            while (matcher.find()) {
                String obtainedRef = matcher.group(1);

                obtainedRef = getValidRef(obtainedRef, ref);

                if (obtainedRef != null) {

                    if (visitedPageSet.add(obtainedRef)) {
                        logger.info("[FIND] " + obtainedRef);
                        needVisitPageSet.add(obtainedRef);
                    }
                }
            }
        }

        return true;
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

        if (!obtainedRef.startsWith(root)) {
            return null;
        }

        if (obtainedRef.equals(server)) {
            return obtainedRef + "/index.php";
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
                    logger.debug("[SKIP] (bad extension '" + ext + "'): " + obtainedRef);
                    return null;
                }
            }
        }

        return obtainedRef;
    }

    public static void main(String[] args) throws IOException {
        DownloadManager downloadManager = new DownloadManager("http://www.sgu.ru", "/home/hd/sgu");
        downloadManager.download();
    }
}
