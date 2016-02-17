package utils;

import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

/**
 * Created by yiguo on 12/15/15.
 */
public class PropertiesReader {
    private static PropertiesObj propertiesObj;

    public static void loadProperties() {
        Yaml yaml = new Yaml();
        try {
            propertiesObj = yaml.loadAs(new FileInputStream(new File(Constants.YAML_CONFIG_PATH)),
                    PropertiesObj.class);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static List<List<String>> getAggregateFieldsList() {
        return propertiesObj.getAggregateFieldsList();
    }

    public static int getMaxBatchSize() {
        return propertiesObj.getMaxBatchSize();
    }

    public static int getMaxSpoutPending() {
        return propertiesObj.getMaxSpoutPending();
    }

    public static List<String> getAcceptedEventTypeList() {
        return propertiesObj.getAcceptedEventTypeList();
    }

    public static boolean isAcceptedEventType(String type) {
        String trimType = type.toLowerCase();
        return propertiesObj.getAcceptedEventTypeSet().contains(type)
                || propertiesObj.getAcceptedEventTypeSet().contains(trimType);
    }
}
