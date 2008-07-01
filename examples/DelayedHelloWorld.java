public class DelayedHelloWorld {
    public static Runnable printCallback = new Runnable() {
        public void run() {
            System.out.println("Hello, world!");
            System.exit(0);
        }
    };
    public static void main(String [] args) throws java.io.IOException {
        bamboo.lss.ASyncCore acore = new bamboo.lss.ASyncCoreImpl();
        acore.registerTimer(5000, printCallback);
        acore.asyncMain();
    }
}
