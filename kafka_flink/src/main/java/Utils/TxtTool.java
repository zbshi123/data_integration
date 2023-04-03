package Utils;

import java.io.*;

public class TxtTool {
    public static void main(String[] args) {
        readTxt();
    }

    public static void readTxt() {
        //String fileName = "/mnt/SharedFile/test.txt";
        String fileName = "data1.txt";
        File file = new File(fileName);
        BufferedReader reader = null;
        //BufferedWriter whiter = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            //whiter = new BufferedWriter(new FileWriter("data1.txt"));
            String tempString = null;
            int line = 1;
            int count = 0;
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                //System.out.println("line " + line + ": " + tempString);
                //line++;
                if (count % 100000 == 0) {
                    System.out.println(count);
                }
                count++;
            }
            System.out.println("end: " + count);
            //whiter.close();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
            /*
            if (null != whiter) {
                try {
                    whiter.close();
                } catch (IOException e2) {
                    e2.printStackTrace();
                }
            }

             */
        }
    }

    public static void writeTxt() {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter("data1.txt"));
            out.write("菜鸟教程" + System.lineSeparator());
            out.write("fdsfs");
            out.close();
            System.out.println("文件创建成功！");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != out) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
