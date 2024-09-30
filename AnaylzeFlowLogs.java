import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class AnaylzeFlowLogs {
    public static final int TCP = 6;
    public static final int LEAF_1 = 25;
    public static final int COMPAQ_PEER = 110;
    public static final String UNTAGGED = "Untagged";

    public static List<LookupTableItem> lookupTable = new ArrayList<>();
    static HashMap<String,String> lookupMap = new HashMap<>();
    static HashMap<String,Integer> outputMap1 = new HashMap<>();
    static HashMap<String,Integer> outputMap2 = new HashMap<>();

    public static void main(String[] args) {
        createLookupTable(args[1]);
        try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            String line;
            String[] logArray;
            while ((line = br.readLine()) != null) {
                logArray = line.split(" ");
                populateLog(logArray);
            }
        } catch (IOException e) {
            System.out.println("Exception opening/reading " + args[0] + ": " + e.getMessage());
        }
        // The dstport and protocol combination decide what tag can be applied.
        writeTagCountFile(outputMap1);
        writePortProtocolCountFile(outputMap2);
    } // main

    public static void createLookupTable(String file) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            String[] logArray;
            LookupTableItem item = new LookupTableItem();

            while ((line = br.readLine()) != null) {
                logArray = line.split(",");
                item.dstPort = logArray[0];
                item.protocol = logArray[1].trim().toLowerCase();
                item.tag = logArray[2];
                lookupTable.add(item);
                lookupMap.put(item.dstPort+item.protocol, item.tag);
            }
        } catch (IOException e) {
            System.out.println("Exception opening/reading " + file + ": " + e.getMessage());
        }
    } // createLookupTable()

    public static void populateLog(String[] logArray) {
        FlowDataItem item = new FlowDataItem();
        item.version = Integer.parseInt(logArray[0]);
        item.accountId = logArray[1];
        item.interfaceId = logArray[2];
        item.srcAddr = logArray[3];
        item.destAddr = logArray[4];
        item.srcPort = Integer.parseInt(logArray[5]);
        item.destPort = Integer.parseInt(logArray[6]);
        item.protocol = getProtocolString(logArray[7]);
        item.packets = Long.parseLong(logArray[8]);
        item.bytes = Long.parseLong(logArray[9]);
        item.start = Long.parseLong(logArray[10]);
        item.end = Long.parseLong(logArray[11]);
        item.action = logArray[12];
        item.status = logArray[13];

        if (item.action.equals("ACCEPT") && item.status.equals("OK")){
            createTagCountMap(item);
            createPortProtocolMap(item);
        }
    } // populateLog()

    public static String getProtocolString(String protocol) {
        String protocolStr;
        switch(Integer.parseInt(protocol)) {
            case TCP:
                protocolStr = "tcp";
                break;
            case LEAF_1:
                protocolStr = "leaf-1";
                break;
            case COMPAQ_PEER:
                protocolStr = "compaq-peer";
                break;
            default:
                System.out.println("Unknown protocol: " + protocol);
                protocolStr = "Unknown protocol";
        }
        return protocolStr;
    } // getProtocolString()

    /*
        Count of matches for each tag
     */
    private static void createTagCountMap(FlowDataItem flowData) {
    //    System.out.println("Matching tags: flowdata size " + flowData.size());
        String dstportProtocol;

        dstportProtocol = flowData.destPort + flowData.protocol;

        if (lookupMap.containsKey(dstportProtocol)) {
            if (outputMap1.containsKey(lookupMap.get(dstportProtocol))) {
                outputMap1.compute(lookupMap.get(dstportProtocol), (k, count) -> count + 1);
            } else {
                outputMap1.put(lookupMap.get(dstportProtocol), 1);
            }
        } else {
            if (outputMap1.containsKey(UNTAGGED)) {
                outputMap1.compute(UNTAGGED, (k, count) -> count + 1);
            } else {
                outputMap1.put(UNTAGGED, 1);
            }
        }
    } // createTagCountMap()

    private static void writeTagCountFile(HashMap<String, Integer> outputMap1) {
        try {
            BufferedWriter buffer = new BufferedWriter(new FileWriter("tagCounts.txt"));
            Set<String> keys = outputMap1.keySet();
            for (String key: keys) {
                buffer.write(key + ", " + outputMap1.get(key).toString() + "\n");
            }
            buffer.flush();
        } catch (Exception e) {
            System.out.println("Exception opening/writing to tagCounts.txt: " + e.getMessage());
        }
    }

    public static void createPortProtocolMap(FlowDataItem flowData) {
        /*
            Count of matches for each port/protocol combination
            Port/Protocol Combination Counts:
            Port,Protocol,Count
         */
        String portProtocol;
        Integer count = 1;
        portProtocol = flowData.destPort + flowData.protocol;
        if (outputMap2.containsKey(portProtocol)) {
            count = outputMap2.get(portProtocol);
            outputMap2.put(flowData.destPort + "," + flowData.protocol, count + 1);
        } else {
            outputMap2.put(flowData.destPort + "," + flowData.protocol, count);
        }
    } // createPortProtocolMap()

    private static void writePortProtocolCountFile(HashMap<String, Integer> outputMap2) {
        Set<String> keys = outputMap2.keySet();
        try {
            BufferedWriter buffer = new BufferedWriter(new FileWriter("portProtocolCounts.txt"));

            for (String key: keys) {
                buffer.write(key + ", " + outputMap2.get(key).toString() + "\n");
            }
            buffer.flush();
        } catch (Exception e) {
            System.out.println("Exception opening/writing to portProtocolCounts.txt: " + e.getMessage());
        }
    }
}
