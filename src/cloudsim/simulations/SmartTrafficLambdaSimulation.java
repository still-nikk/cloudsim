package cloudsim.simulations;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.util.*;

public class SmartTrafficLambdaSimulation {

    private static final double WARM_START_DURATION = 5.0;

    public static void main(String[] args) {
        try {
            int numUsers = 1;
            Calendar calendar = Calendar.getInstance();
            boolean traceFlag = false;

            CloudSim.init(numUsers, calendar, traceFlag);

            Datacenter datacenter = createDatacenter("Datacenter_0");

            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();

            List<Vm> vmList = new ArrayList<>();
            Vm vm1 = new Vm(0, brokerId, 1000, 2, 1024, 10000, 10000,
                    "Xen", new CloudletSchedulerTimeShared());
            Vm vm2 = new Vm(1, brokerId, 1000, 2, 1024, 10000, 10000,
                    "Xen", new CloudletSchedulerTimeShared());
            vmList.add(vm1);
            vmList.add(vm2);

            int nextVmId = 2;
            List<CloudletExecutionInfo> executionSchedule = createExecutionSchedule(brokerId);

            Map<Integer, Map<Integer, Double>> warmFunctions = new HashMap<>();
            Map<Integer, Boolean> warmStartStatus = new HashMap<>();
            Map<Integer, List<MemoryUsageRecord>> vmMemoryUsage = new HashMap<>();

            for (Vm vm : vmList) {
                vmMemoryUsage.put(vm.getId(), new ArrayList<>());
            }

            broker.submitVmList(vmList);
            executionSchedule.sort(Comparator.comparingDouble(CloudletExecutionInfo::getExecutionTime));

            double currentTime = 0.0;

            for (CloudletExecutionInfo info : executionSchedule) {
                Cloudlet cloudlet = info.getCloudlet();
                int functionType = info.getFunctionType();
                currentTime = info.getExecutionTime();

                System.out.println("\nTime: " + currentTime + " - Processing cloudlet " +
                        cloudlet.getCloudletId() + " (Function Type: " + functionType + ")");

                boolean foundWarmVM = false;
                int vmId = -1;

                if (warmFunctions.containsKey(functionType)) {
                    Map<Integer, Double> vmExecutionTimes = warmFunctions.get(functionType);

                    for (Map.Entry<Integer, Double> entry : new ArrayList<>(vmExecutionTimes.entrySet())) {
                        int candidateVmId = entry.getKey();
                        double lastExecutionTime = entry.getValue();

                        if (currentTime - lastExecutionTime <= WARM_START_DURATION) {
                            double currentMemoryUsage = calculateCurrentMemoryUsage(currentTime, vmMemoryUsage.get(candidateVmId));
                            double memoryNeeded = 0.4 * 1024;

                            Vm selectedVm = null;
                            for (Vm vm : vmList) {
                                if (vm.getId() == candidateVmId) {
                                    selectedVm = vm;
                                    break;
                                }
                            }

                            if (selectedVm != null && (currentMemoryUsage + memoryNeeded) <= selectedVm.getRam()) {
                                vmId = candidateVmId;
                                foundWarmVM = true;
                                warmStartStatus.put(cloudlet.getCloudletId(), true);
                                break;
                            }
                        }
                    }
                }

                if (!foundWarmVM) {
                    double memoryNeeded = 0.4 * 1024;
                    boolean vmFound = false;

                    for (Vm vm : vmList) {
                        double currentMemoryUsage = calculateCurrentMemoryUsage(currentTime, vmMemoryUsage.get(vm.getId()));
                        if ((currentMemoryUsage + memoryNeeded) <= vm.getRam()) {
                            vmId = vm.getId();
                            vmFound = true;
                            break;
                        }
                    }

                    if (!vmFound) {
                        Vm newVm = new Vm(nextVmId, brokerId, 1000, 2, 1024, 10000, 10000,
                                "Xen", new CloudletSchedulerTimeShared());
                        vmList.add(newVm);
                    
                        List<Vm> tempList = new ArrayList<>();
                        tempList.add(newVm);
                        broker.submitVmList(tempList);
                    
                        vmMemoryUsage.put(nextVmId, new ArrayList<>());
                        vmId = nextVmId;
                        nextVmId++;
                    }
                    
                    warmStartStatus.put(cloudlet.getCloudletId(), false);
                }

                cloudlet.setVmId(vmId);

                warmFunctions.putIfAbsent(functionType, new HashMap<>());
                warmFunctions.get(functionType).put(vmId, currentTime);

                double executionLength = warmStartStatus.get(cloudlet.getCloudletId()) ? 1.0 : 2.0;
                cloudlet.setCloudletLength((long) (executionLength * 500));

                double memoryUsage = 0.4 * 1024;
                vmMemoryUsage.get(vmId).add(new MemoryUsageRecord(currentTime, memoryUsage, true));
                vmMemoryUsage.get(vmId).add(new MemoryUsageRecord(currentTime + executionLength, memoryUsage, false));

                info.setVmId(vmId);
                info.setWarmStart(warmStartStatus.get(cloudlet.getCloudletId()));
                info.setFinishTime(currentTime + executionLength);
            }

            List<Cloudlet> cloudletList = new ArrayList<>();
            for (CloudletExecutionInfo info : executionSchedule) {
                cloudletList.add(info.getCloudlet());
            }

            broker.submitCloudletList(cloudletList);
            List<TrafficEvent> trafficEvents = new ArrayList<>();
            trafficEvents.add(new TrafficEvent("Junction A", "congestion", 1.5));
            trafficEvents.add(new TrafficEvent("Highway 7", "accident", 3.0));
            trafficEvents.add(new TrafficEvent("Junction B", "normal", 5.5));

System.out.println("\n=== Simulated Traffic Events ===");
for (TrafficEvent event : trafficEvents) {
    System.out.println(event);
}
System.out.println("================================\n");
            System.out.println("\nStarting CloudSim simulation...");
            CloudSim.startSimulation();
            CloudSim.stopSimulation();

            System.out.println("Simulation completed.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Datacenter createDatacenter(String name) throws Exception {
        List<Host> hostList = new ArrayList<>();

        List<Pe> peList = new ArrayList<>();
        peList.add(new Pe(0, new PeProvisionerSimple(2000))); // PE id = 0
        peList.add(new Pe(1, new PeProvisionerSimple(2000))); // PE id = 1
        peList.add(new Pe(2, new PeProvisionerSimple(2000))); // Add more if needed

        int hostId = 0;
        int ram = 4096;
        long storage = 1000000;
        int bw = 50000;

        hostList.add(new Host(hostId, new RamProvisionerSimple(ram),
                new BwProvisionerSimple(bw), storage, peList,
                new VmSchedulerTimeShared(peList)));

        return new Datacenter(name, new DatacenterCharacteristics("x86", "Linux", "Xen",
                hostList, 10.0, 3.0, 0.05, 0.1, 0.1), new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);
    }

    private static DatacenterBroker createBroker() throws Exception {
        return new DatacenterBroker("Broker");
    }

    private static Cloudlet createCloudlet(int id, int brokerId) {
        long length = 1000;
        int pesNumber = 1;
        long fileSize = 300;
        long outputSize = 300;
        UtilizationModel utilizationModel = new UtilizationModelFull();

        Cloudlet cloudlet = new Cloudlet(id, length, pesNumber, fileSize, outputSize,
                utilizationModel, utilizationModel, utilizationModel);
        cloudlet.setUserId(brokerId);
        return cloudlet;
    }

    private static double calculateCurrentMemoryUsage(double time, List<MemoryUsageRecord> records) {
        double total = 0.0;
        for (MemoryUsageRecord r : records) {
            if (r.isStart() && r.getTime() <= time) {
                total += r.getMemory();
            } else if (!r.isStart() && r.getTime() <= time) {
                total -= r.getMemory();
            }
        }
        return total;
    }

    private static List<CloudletExecutionInfo> createExecutionSchedule(int brokerId) {
        List<CloudletExecutionInfo> executionSchedule = new ArrayList<>();

        for (int i = 0; i <= 5; i++) {
            Cloudlet cloudlet = createCloudlet(i, brokerId);
            executionSchedule.add(new CloudletExecutionInfo(cloudlet, 0, i));
        }

        Cloudlet cloudlet2Again = createCloudlet(6, brokerId);
        executionSchedule.add(new CloudletExecutionInfo(cloudlet2Again, 3.0, 2));

        Cloudlet cloudlet4Again = createCloudlet(7, brokerId);
        executionSchedule.add(new CloudletExecutionInfo(cloudlet4Again, 6.0, 4));

        return executionSchedule;
    }
}

// === Supporting Classes ===

class CloudletExecutionInfo {
    private Cloudlet cloudlet;
    private double executionTime;
    private int functionType;
    private int vmId;
    private boolean warmStart;
    private double finishTime;

    public CloudletExecutionInfo(Cloudlet cloudlet, double executionTime, int functionType) {
        this.cloudlet = cloudlet;
        this.executionTime = executionTime;
        this.functionType = functionType;
    }

    public Cloudlet getCloudlet() {
        return cloudlet;
    }

    public double getExecutionTime() {
        return executionTime;
    }

    public int getFunctionType() {
        return functionType;
    }

    public void setVmId(int vmId) {
        this.vmId = vmId;
    }

    public void setWarmStart(boolean warmStart) {
        this.warmStart = warmStart;
    }

    public void setFinishTime(double finishTime) {
        this.finishTime = finishTime;
    }
}

class MemoryUsageRecord {
    private double time;
    private double memory;
    private boolean start;

    public MemoryUsageRecord(double time, double memory, boolean start) {
        this.time = time;
        this.memory = memory;
        this.start = start;
    }

    public double getTime() {
        return time;
    }

    public double getMemory() {
        return memory;
    }

    public boolean isStart() {
        return start;
    }
}
class TrafficEvent {
    private String location;
    private String eventType; // e.g., "accident", "congestion", "normal"
    private double time;      // time of event

    public TrafficEvent(String location, String eventType, double time) {
        this.location = location;
        this.eventType = eventType;
        this.time = time;
    }

    public String getLocation() {
        return location;
    }

    public String getEventType() {
        return eventType;
    }

    public double getTime() {
        return time;
    }

    @Override
    public String toString() {
        return "[Time " + time + "] Event at " + location + ": " + eventType.toUpperCase();
    }
}
