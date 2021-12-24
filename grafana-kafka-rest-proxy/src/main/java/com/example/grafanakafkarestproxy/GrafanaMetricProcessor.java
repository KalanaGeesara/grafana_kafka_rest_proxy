package com.accelaero.ops.manager.integration.adapter;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.util.*;

@Component(value = "GrafanaMetricProcessorKafka")
public class GrafanaMetricProcessor {

    public String getValue(String changeMessage, String key) throws JSONException {

        String value = "";

        switch (key) {
            case "Status":
                String description = (String) new JSONObject(changeMessage).get("description");
                String[] descriptionArr  =description.split("\n");
                value = descriptionArr[descriptionArr.length-1].split(":")[1];
                break;
            case "flightReference":
                String details = (String) new JSONObject(changeMessage).get("details");
                value = details.split("\n")[2].split(" ")[0];
                break;

        }
        return value;
    }

    public List<String> getGeneratedMessages(String consumedMessage) throws JSONException {

        List<String> generatedMessages = new ArrayList<>();
        String details = (String) new JSONObject(consumedMessage).get("details");

        ArrayList<String> arr = new ArrayList<String>( Arrays.asList(details.split("\n")));

        String fristPart = consumedMessage.split("Triggered metrics:")[0];
        String lastPart = consumedMessage.split("Triggered metrics:")[1].split("incident_key")[1];

        arr.stream().filter(m -> !(m.equals("") || m.contains("Triggered metrics"))).forEach(element -> {
            generatedMessages.add(fristPart + "Triggered metrics:\\n\\n" + element + "\",\"incident_key" + lastPart);
        });


        return generatedMessages;


    }
}
