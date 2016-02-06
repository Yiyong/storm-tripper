package utils;

import model.TripperPropObj;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.shade.org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by yiguo on 12/15/15.
 */
public class PropertiesReader {
    private static PropertiesReader propertiesReader;
    private sta TripperPropObj tripperPropObj;

    private void init() {
        Yaml yaml = new Yaml();
        try {
            this.tripperPropObj = yaml.loadAs(new FileInputStream(new File(Constants.YAML_CONFIG_PATH)),
                    TripperPropObj.class);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private PropertiesReader() {
        init();
    }

    public static PropertiesReader getPropertiesReader() {
        if (propertiesReader == null) {
            propertiesReader = new PropertiesReader();
        }
        return propertiesReader;
    }

    public List<List<String>> getAggregateFieldsList() {
        return tripperPropObj.getAggregateFieldsList();
    }

    public int getMaxBatchSize() {
        return tripperPropObj.getMaxBatchSize();
    }

    public int getMaxSpoutPending() {
        return tripperPropObj.getMaxSpoutPending();
    }
}
