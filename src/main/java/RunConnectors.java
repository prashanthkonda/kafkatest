
import io.confluent.connect.jdbc.JdbcSinkConnector;
import io.confluent.connect.jdbc.JdbcSourceConnector;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;


public class RunConnectors
{


    private static final String SOURCE_PATH = "source-quickstart-sqlite.properties";
    private static final String SINK_PATH = "sink-quickstart-sqlite.properties";


    public static void main(String[] args) {
        JdbcSourceConnector sourceConnector = new JdbcSourceConnector();

        try (InputStream stream = RunConnectors.class.getResourceAsStream(SOURCE_PATH)) {
            Properties props = new Properties() ;
            props.load(stream);

            Map<String, String> map = new HashMap<String, String> ();

            map.putAll(props.entrySet()
                    .stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(),
                            e -> e.getValue().toString())));

            sourceConnector.start(map);

        } catch (Exception e) {
            e.printStackTrace();
        }


        JdbcSinkConnector sinkConnector = new JdbcSinkConnector();

        try (InputStream stream = RunConnectors.class.getResourceAsStream(SINK_PATH)) {
            Properties props = new Properties() ;
            props.load(stream);

            Map<String, String> map = new HashMap<String, String>();

            map.putAll(props.entrySet()
                    .stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(),
                            e -> e.getValue().toString())));

            sinkConnector.start(map);

        } catch (Exception e) {
            e.printStackTrace();
        }


        try {
            Thread.sleep(400000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        sourceConnector.stop();
        sinkConnector.stop();

    }

}
