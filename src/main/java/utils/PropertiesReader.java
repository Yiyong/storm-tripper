package utils;

import model.TripperPropObj;
import org.apache.log4j.Logger;
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
    private static TripperPropObj tripperPropObj;

    private static List<List<String>> aggregateFieldsList;

    private void init() {
        this.tripperPropObj = new TripperPropObj();
        Constructor STPConstructor = new Constructor(TripperPropObj.class);
        Yaml STPYaml = new Yaml(STPConstructor);

        try {
            this.tripperPropObj = (TripperPropObj) STPYaml.load(new FileInputStream(new File(Constants.YAML_CONFIG_PATH)));
            aggregateFieldsList = new LinkedList<List<String>>();
            for(String aggregateFields : tripperPropObj.getAggregateFieldsList()){
                String[] aggregateFieldsArray = aggregateFields.split(Constants.AGGREGATE_FIELD_DELIMITER);
                aggregateFieldsList.add(Arrays.asList(aggregateFieldsArray));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private PropertiesReader(){
        init();
    }

    public static PropertiesReader getPropertiesReader() {
        if(propertiesReader == null){
            propertiesReader = new PropertiesReader();
        }
        return propertiesReader;
    }

    public static List<List<String>> getAggregateFieldsList() {
        if(propertiesReader == null){
            propertiesReader = new PropertiesReader();
        }
        return aggregateFieldsList;
    }
}
