
/**
 * @ClassName gmall-parent-test2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月30日10:20 - 周二
 * @Describe
 */
public class test2 {
    public static void main(String[] args) throws InterruptedException {
        long num = 1;
        while (true) {
            System.out.println("数据来了" + num++);
            Thread.sleep((int) (Math.random() * (2000 - 100 + 1) + 100));
        }
    }
}
