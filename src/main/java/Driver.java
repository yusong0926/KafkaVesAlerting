package main.java;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import evel_javalibrary.att.com.*;

import org.apache.log4j.Level;

import java.io.*;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;

import java.util.*;
import java.util.Properties;

import java.io.IOException;
import java.util.Map;


public class Driver
{
    private static Logger logger = Logger.getLogger(Driver.class);

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream("config.properties");

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            String event_api_url = prop.getProperty("event_api_url");
            int port = Integer.parseInt(prop.getProperty("port"));
            String path = prop.getProperty("path");
            String topic = prop.getProperty("topic");
            String username = prop.getProperty("username");
            String password = prop.getProperty("password");
            int totalCapacity = Integer.parseInt(prop.getProperty("total_capacity"));
            try {

                AgentMain.evel_initialize(event_api_url, port,
                        null, topic,
                        username,
                        password,
                        Level.DEBUG);
                System.out.println("topic: " + topic);
            } catch (Exception e) {
                e.printStackTrace();
            }

            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "alert");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("event-raw"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    long startTime = System.nanoTime();
                    //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    String value = record.value();
                    int ind = value.indexOf(",");
                    String type = value.substring(0, ind);
                    String payload = value.substring(ind + 1, value.length());
                    Gson gson = new Gson();
                    Type stringStringMap = new TypeToken<Map<String, String>>() {
                    }.getType();

                    Map<String, String> map = gson.fromJson(payload, stringStringMap);
                    if (type.equals("enodeb")) {
                        if (alert(map, totalCapacity)) {
                            sendAlert(map);
                        }
                    }
                    long duration = System.nanoTime() - startTime;
                }


            }

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static boolean alert(Map<String, String> map, int totalCapacity) {
        if (Integer.valueOf(map.get("dlbitrate"))/1000 > totalCapacity * 0.8 * Integer.valueOf(map.get("dlallocrbrate"))/100.0) return true;
        if (Integer.valueOf(map.get("ulbitrate"))/1000 > totalCapacity * 0.8 * Integer.valueOf(map.get("ulallocrbrate"))/100.0) return true;
        return false;
    }

    public static void sendAlert(Map<String, String> map) {
        logger.info("start sending alert");
        logger.debug("alerting events: " + map.toString());
        Date dNow = new Date( );
        SimpleDateFormat ft = new SimpleDateFormat ("E, MM dd yyyy hh:mm:ss zzz");
        Map<String, String> map_header = new HashMap<>();
        map_header.put("collectorTimeStamp", ft.format(dNow));
        Gson gson = new Gson();
        String internalHeader = gson.toJson(map_header);
        
        String eid = "Progran-20";

        EvelProgranProfile flt  = new EvelProgranProfile("enodbMeasurement", eid,"Resource Management", "MEASUREMENT",
                EvelHeader.PRIORITIES.EVEL_PRIORITY_HIGH,
                EvelProgranProfile.EVEL_SEVERITIES.EVEL_SEVERITY_CRITICAL,
                EvelProgranProfile.EVEL_SOURCE_TYPES.EVEL_SOURCE_VPROGRAN,
                EvelProgranProfile.EVEL_VF_STATUSES.EVEL_VF_STATUS_ACTIVE,
                internalHeader);
        flt.evel_header_set_source_name("PROGRAN");
        flt.evel_header_set_sourceid(true,"Progran");
        flt.evel_nfcnamingcode_set("progran");
        flt.evel_nfnamingcode_set("vPROGRAN");
        flt.evel_reporting_entity_id_set("No UUID available");
        flt.evel_reporting_entity_name_set(map.get("profile"));
        flt.evel_fault_interface_set("ProgranInt");

        flt.evel_start_epoch_set((long)(Double.parseDouble(map.get("time"))*1000));
        flt.evel_last_epoch_set((long)(Double.parseDouble(map.get("time"))*1000));
        flt.evel_fault_addl_info_add("time", "time-"+(long)(Double.parseDouble(map.get("time"))*1000));
        flt.evel_fault_addl_info_add("dlBitrate", "dlBitrate-"+map.get("dlbitrate"));
        flt.evel_fault_addl_info_add("ulBitrate", "ulBitrate-"+map.get("ulbitrate"));
        flt.evel_fault_addl_info_add("dlAllocRBRate", "dlAllocRBRate-"+map.get("dlallocrbrate"));
        flt.evel_fault_addl_info_add("ulAllocRBRate", "ulAllocRBRate-"+map.get("ulallocrbrate"));

        AgentMain.evel_post_event(flt);
    }
}

