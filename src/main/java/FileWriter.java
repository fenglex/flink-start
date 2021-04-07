import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.RandomUtil;

/**
 * @author haifeng
 * @version 1.0
 * @date 2021/3/18 14:54
 */
public class FileWriter {
    public static void main(String[] args) {

        String file = "D:\\Soft\\Code\\flink-start\\a.txt";
        FileUtil.del(file);
        int row = 20;
        String chars = "abcdefgilknvuasdfmlkviouasndfashypm";
        for (int i = 0; i < row; i++) {
            int anInt = RandomUtil.randomInt(0, chars.length());
            String content = chars.charAt(anInt) + "," + RandomUtil.randomInt(1000)+"\n";
            FileUtil.appendString(content, file, "utf-8");
        }

    }
}
