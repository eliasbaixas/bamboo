
import bamboo.router.Router;
import bamboo.util.StandardStage;
import seda.sandStorm.api.ConfigDataIF;

public class NewerSimpleStage extends StandardStage {

    protected Router router;

    public void init(ConfigDataIF config) throws Exception {
        super.init(config);
        acore.registerTimer(0, ready);
    }

    protected Runnable ready = new Runnable() {
        public void run() {
            router = Router.instance(my_node_id);
        }
    };
}

