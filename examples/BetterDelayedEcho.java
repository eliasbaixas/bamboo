public class BetterDelayedEcho {
    public static void main(String [] args) throws java.io.IOException {
        if (args.length < 1) {
            System.err.println("usage: java BetterDelayedEcho <string>");
            System.exit(1);
        }
        bamboo.lss.ASyncCore acore = new bamboo.lss.ASyncCoreImpl();
        final String str = args[0];
        Runnable printCallback = new Runnable() {
            public void run() {
                System.out.println(str);
                System.exit(0);
            }
        };
        acore.registerTimer(5000, printCallback);
        acore.asyncMain();
    }
}
