import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.Arrays;
import java.util.Collections;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class LeMansRace implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    LeMansRace(String address) {
        if(zk == null){
            try {
                System.out.println("Iniciando conexão com o ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = Integer.valueOf(-1);
                System.out.println("Finalizada a inicialização do ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
        }
    }

    static public class Barrier extends LeMansRace {
        int size;
        String name;

        Barrier(String address, String root, int size) {
            super(address);
            this.root = root;
            this.size = size;

            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Exceção do Keeper ao instanciar a barreira: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Exceção de interrupção");
                }
            }

            try {
                name = InetAddress.getLocalHost().getCanonicalHostName().toString();
            } catch (UnknownHostException e) {
                System.out.println(e.toString());
            }
        }

        boolean enter() throws KeeperException, InterruptedException{
            zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);

                    if (list.size() < size) {
                        mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
        }
        
        boolean leave() throws KeeperException, InterruptedException{
            String myName = "";
            zk.delete(root + "/" + myName, 0);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                        if (list.size() > 0) {
                            mutex.wait();
                        } else {
                            return true;
                        }
                    }
                }
        }
    }

    static public class TimeTracker extends LeMansRace {

        TimeTracker(String address, String name) {
            super(address);
            this.root = name;

            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Exceção do Keeper ao instanciar a fila: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Exceção de interrupção");
                }
            }
        }

        boolean createTime(int timeNumber) throws KeeperException, InterruptedException{
            ByteBuffer b = ByteBuffer.allocate(4);
            byte[] value;

            b.putInt(timeNumber);
            value = b.array();
            zk.create(root + "/tempo-", value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }

        String completeTime() throws KeeperException, InterruptedException{
            Stat stat = null;

            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() == 0) {
                        System.out.println("O timer chegou ao fim, corrida finalizada.");
                        mutex.wait();
                    } else {
                        Collections.sort(list);
                        String timeNode = list.get(0);
                        
                        System.out.println("Tempo a ser completado: " + timeNode);
                        zk.getData(root + "/" + timeNode, false, stat);
                        zk.delete(root + "/" + timeNode, -1);
                        return timeNode;
                    }
                }
            }
        }
    }

    static public class TrackLock extends LeMansRace {
        String root;
        String pathName; 

        TrackLock(String address, String name) {
            super(address);
            this.root = name;
        }

        public void acquireTrack() throws KeeperException, InterruptedException {
            pathName = zk.create(root + "/lock-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("Tentando adquirir o lock da pista com o nó " + pathName);

            while (true) {
                synchronized (mutex) {
                    List<String> children = zk.getChildren(root, false);
                    Collections.sort(children);

                    String myNode = pathName.substring(root.length() + 1);

                    if (myNode.equals(children.get(0))) {
                        System.out.println(myNode + " adquiriu a pista!");
                        return;
                    }

                    int myIndex = children.indexOf(myNode);
                    String predecessor = children.get(myIndex - 1);

                    Stat stat = zk.exists(root + "/" + predecessor, true); 

                    if (stat != null) {
                        System.out.println(myNode + " está esperando por " + predecessor);
                        mutex.wait();
                    }
                }
            }
        }

        public void releaseTrack() throws KeeperException, InterruptedException {
            System.out.println("Liberando o lock da pista em " + pathName);
            zk.delete(pathName, -1);
        }
    }
    
    public static void main(String args[]) {
        if (args.length == 0) {
            System.err.println("Por favor, especifique uma opção: timeProducer, racer");
            return;
        }
        if (args[0].equals("timeProducer"))
            timeProducerTest(args);
        else if (args[0].equals("racer"))
            racerProcess(args);
        else
        	System.err.println("Opção desconhecida: " + args[0]);
    }

    public static void timeProducerTest(String args[]) {
        TimeTracker lt = new TimeTracker(args[1], "/timer");
        Integer max = Integer.valueOf(args[2]);

        System.out.println("Produtor do Timer");
		for (int i = 0; i < max; i++)
			try{
				lt.createTime(i + 1);
			} catch (KeeperException | InterruptedException e){
				e.printStackTrace();
			}
    }
    
    public static void racerProcess(String args[]) {
        if (args.length < 3) {
            System.err.println("USO: racer <endereco_zk> <numero_de_corredores>");
            return;
        }
        String zkAddress = args[1];
        int racerCount = Integer.valueOf(args[2]);
        String barrierRoot = "/starting-grid";
        String queueRoot = "/timer";
        String lockRoot = "/track-lock";
        String electionRoot = "/race-election";
        String racerId = "racer-" + new Random().nextInt(100000);
        System.out.println("Corredor " + racerId + " iniciando...");

        new LeMansRace(zkAddress);
        try {
            while (zk == null || !zk.getState().isConnected()) {
                Thread.sleep(100);
            }
            for (String path : Arrays.asList(barrierRoot, queueRoot, lockRoot, electionRoot)) {
                if (zk.exists(path, false) == null) {
                    zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return;
        }

        Barrier b = new Barrier(zkAddress, barrierRoot, racerCount);
        try {
            System.out.println(racerId + " está esperando no grid de largada " + barrierRoot);
            b.enter();
            System.out.println(racerId + " passou do grid de largada. Pronto para correr!");
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return;
        }

        String myNodePath;
        try {
            myNodePath = createElectionNode(electionRoot, racerId);
            System.out.println(racerId + " entrou na eleição com o nó: " + myNodePath.substring(electionRoot.length() + 1));
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Corredor " + racerId + " não conseguiu entrar na eleição.");
            e.printStackTrace();
            return;
        }

        while (true) {
            try {
                if (isLeader(electionRoot, myNodePath)) {
                    System.out.println(">>>> " + racerId + " assumiu a LIDERANÇA! <<<<");
                    raceAsLeaderLoop(new TimeTracker(zkAddress, queueRoot), new TrackLock(zkAddress, lockRoot), racerId);
                    break;
                } else {
                    watchPredecessor(electionRoot, myNodePath);
                    System.out.println(racerId + ": O líder/corredor cansou. Tentando nova eleição...");
                }
            } catch (KeeperException.SessionExpiredException e) {
                System.err.println("Sessão com o Zookeeper expirou. Encerrando.");
                return;
            } catch (KeeperException | InterruptedException e) {
                System.err.println("Corredor " + racerId + " encontrou um erro. Reiniciando checagem em 5s.");
                e.printStackTrace();
                try { Thread.sleep(5000); } catch (InterruptedException ie) {}
            }
        }
    }

    public static void raceAsLeaderLoop(TimeTracker TimeTracker, TrackLock trackLock, String racerId) throws KeeperException, InterruptedException {
        while (true) {
            String time = TimeTracker.completeTime();

            trackLock.acquireTrack();
            System.out.println("CORREDOR LÍDER (" + racerId + ") está na pista (simulando 1h de trabalho)...");
            Thread.sleep(10000);
            System.out.println(">>>> CORREDOR LÍDER (" + racerId + "): finalizou o tempo " + time + " <<<<");
            trackLock.releaseTrack();
        }
    }

    public static String createElectionNode(String electionRoot, String racerId) throws KeeperException, InterruptedException {
        return zk.create(electionRoot + "/node-", racerId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public static boolean isLeader(String electionRoot, String myNodePath) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(electionRoot, false);
        Collections.sort(children);
        String myNodeName = myNodePath.substring(electionRoot.length() + 1);
        return children.get(0).equals(myNodeName);
    }

    public static void watchPredecessor(String electionRoot, String myNodePath) throws KeeperException, InterruptedException {
        String myNodeName = myNodePath.substring(electionRoot.length() + 1);
        
        while(true) {
            List<String> children = zk.getChildren(electionRoot, false);
            Collections.sort(children);
            int myIndex = children.indexOf(myNodeName);

            if (myIndex == -1) {
                 System.err.println("Não encontrei meu nó ("+myNodeName+"). Saindo da vigília.");
                 return;
            }
            if (myIndex == 0) {
                return;
            }

            String predecessorName = children.get(myIndex - 1);
            
            synchronized (mutex) {
                Stat stat = zk.exists(electionRoot + "/" + predecessorName, true);
                if (stat != null) {
                    System.out.println("Corredor seguidor " + myNodeName + " está vigiando " + predecessorName);
                    mutex.wait(); 
                    return; 
                }
            }
        }
    }
}