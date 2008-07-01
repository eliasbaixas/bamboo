import static bamboo.util.Curry.*;
public class DelayedEcho {
    public static Thunk1<String> printCallback = new Thunk1<String>() {
        public void run(String str) {
            System.out.println(str);
            System.exit(0);
        }
    };
    public static void main(String [] args) throws java.io.IOException {
        if (args.length < 1) {
            System.err.println("usage: java DelayedEcho <string>");
            System.exit(1);
        }
        bamboo.lss.ASyncCore acore = new bamboo.lss.ASyncCoreImpl();
        acore.registerTimer(5000, curry(printCallback, args[0]));
        acore.asyncMain();
    }
}
