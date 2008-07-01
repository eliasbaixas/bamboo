public class BadDelayedEcho {
    public static String str;
    public static Runnable printCallback = new Runnable() {
        public void run() {
            System.out.println(str);
            System.exit(0);
        }
    };
    public static void main(String [] args) throws java.io.IOException {
        if (args.length < 1) {
            System.err.println("usage: java BadDelayedEcho <string>");
            System.exit(1);
        }
        str = args[0];
        bamboo.lss.ASyncCore acore = new bamboo.lss.ASyncCoreImpl();
        acore.registerTimer(5000, printCallback);
        acore.asyncMain();
    }
}
