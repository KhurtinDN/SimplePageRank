package ru.sgu.csit.spec.hadoop.downloader;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Scanner;

/**
 * User: KhurtinDN ( KhurtinDN@gmail.com )
 * Date: 12/23/10
 * Time: 12:51 AM
 */
public class IncrementCopy {
    private static final byte[] buffer = new byte[1024];

    private static long name = 0;

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(new FileInputStream("/home/hd/sgu/list.txt"));

        while (scanner.hasNext()) {
            String fileName = scanner.nextLine();

            if (fileName.endsWith("/_file") ||
                    fileName.equals("_file") ||
                    fileName.contains("?") ||
                    fileName.contains("&") ||
                    fileName.contains("question")) { // todo: delete
                continue;
            }

            fileName = "/home/hd/sgu" + fileName.substring(1);

            String page = downloadPage(fileName);
            if (page.length() < 100) {
                System.out.println(fileName + " len = " + page.length());
            } else {
                File file = new File(fileName);
                if (!file.renameTo(new File("/home/hd/sgu_best/" + (name++) + ".html"))) {
                    System.err.println("FAIL file: " + fileName);
                }
            }
        }
    }

    public static String downloadPage(String fileName) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(fileName);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        for (int len; (len = fileInputStream.read(buffer)) > 0;) {
            byteArrayOutputStream.write(buffer, 0, len);
        }

        return new String(byteArrayOutputStream.toByteArray(), Charset.forName("utf8"));
    }
}
