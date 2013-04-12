import devmap.DevMap;

public class Test {
    public static void main(String[] args) {
        if (DevMap.isValid()) {
            System.out.println("DevMap is valid!");
        } else {
            System.out.println("Invalid!");
        }

        DevMap dm = new DevMap();

        dm.refreshDevMap();
        System.out.print(dm.dumpDevMap());
    }
}
