import java.util.LinkedList;
import java.util.List;

/**
 * Created by yifeiliu on 3/17/17.
 * Description:
 */
public class test {

    public static void main(String[] args) {
        List<String> vals = new LinkedList<String>(){{
            add("One");
            add("Two");
            add("Three");
        }};

        System.out.println(vals.toString());
    }
}
