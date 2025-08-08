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

public class SyncPrimitive implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    SyncPrimitive(String address) {
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = Integer.valueOf(-1);
                System.out.println("Finished starting ZK: " + zk);
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

    /**
     * Barrier
     */
    static public class Barrier extends SyncPrimitive {
        int size;
        String name;

        /**
         * Barrier constructor
         */
        Barrier(String address, String root, int size) {
            super(address);
            this.root = root;
            this.size = size;

            // Create barrier node
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating barrier: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }

            // My node name
            try {
                name = InetAddress.getLocalHost().getCanonicalHostName().toString();
            } catch (UnknownHostException e) {
                System.out.println(e.toString());
            }
        }

        /**
         * Join barrier
         */
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

        /**
         * Wait until all reach barrier
         */
        boolean leave() throws KeeperException, InterruptedException{
            // This is the original leave method, not used by the new worker process.
            String myName = ""; // This would need to be stored from the create() call to work correctly.
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

    /**
     * Producer-Consumer queue
     */
    static public class Queue extends SyncPrimitive {

        /**
         * Constructor of producer-consumer queue
         */
        Queue(String address, String name) {
            super(address);
            this.root = name;
            // Create ZK node name
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }

        /**
         * Add element to the queue.
         */
        boolean produce(int i) throws KeeperException, InterruptedException{
            ByteBuffer b = ByteBuffer.allocate(4);
            byte[] value;

            // Add child with value i
            b.putInt(i);
            value = b.array();
            zk.create(root + "/ticket", value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }


        /**
         * Remove first element from the queue.
         */
        String consume() throws KeeperException, InterruptedException{
            int retvalue = -1;
            Stat stat = null;

            // Get the first element available
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() == 0) {
                        System.out.println("Fila vazia, aguardando tickets...");
                        mutex.wait();
                    } else {
                        // Sort to find the smallest sequence number
                        Collections.sort(list);
                        String minString = list.get(0);
                        
                        System.out.println("Ticket a ser consumido: " + minString);
                        byte[] b = zk.getData(root + "/" + minString, false, stat);
                        zk.delete(root + "/" + minString, -1);
                        ByteBuffer buffer = ByteBuffer.wrap(b);
                        retvalue = buffer.getInt();
                        return minString;
                    }
                }
            }
        }
    }

    /**
     * Distributed Lock (Refactored)
     */
    static public class Lock extends SyncPrimitive {
        String root;
        String pathName; // Full path of my lock node, e.g., /work-lock/lock-0000000001

        /**
         * Constructor of lock
         */
        Lock(String address, String name) {
            super(address);
            this.root = name;
            // The parent znode should be created by the worker process before calling this.
        }

        /**
         * Acquires the lock. This method blocks until the lock is acquired.
         */
        public void lock() throws KeeperException, InterruptedException {
            // Create an ephemeral, sequential node under the lock root
            pathName = zk.create(root + "/lock-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("Tentando adquirir lock com o no " + pathName);

            while (true) {
                synchronized (mutex) {
                    List<String> children = zk.getChildren(root, false);
                    Collections.sort(children); // Sort children to find the lowest and the predecessor

                    String myNode = pathName.substring(root.length() + 1);

                    // If my node is the first in the list, I have the lock
                    if (myNode.equals(children.get(0))) {
                        System.out.println(myNode + " adquiriu o lock!");
                        return;
                    }

                    // Otherwise, find my predecessor and set a watch on it
                    int myIndex = children.indexOf(myNode);
                    String predecessor = children.get(myIndex - 1);

                    Stat stat = zk.exists(root + "/" + predecessor, true); // Set watcher on predecessor

                    if (stat != null) {
                        System.out.println(myNode + " esta esperando por " + predecessor);
                        mutex.wait(); // Wait for the notification from the watcher
                    }
                    // If stat is null, the predecessor disappeared. The loop will run again.
                }
            }
        }

        /**
         * Releases the lock.
         */
        public void unlock() throws KeeperException, InterruptedException {
            System.out.println("Liberando o lock em " + pathName);
            zk.delete(pathName, -1);
        }
    }

    // The original Leader class is left unchanged as it's not directly used by the worker.
	static public class Leader extends SyncPrimitive {
    	String leader;
    	String id; //Id of the leader
    	String pathName;
    	
        Leader(String address, String name, String leader, int id) {
            super(address);
            this.root = name;
            this.leader = leader;
            this.id = Integer.valueOf(id).toString();
            // ... (original implementation)
        }
        
        boolean elect() throws KeeperException, InterruptedException{
        	this.pathName = zk.create(root + "/w-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        	System.out.println("My path name is: "+pathName+" and my id is: "+id+"!");
        	return check();
        }
        
        boolean check() throws KeeperException, InterruptedException{
        	Integer suffix = Integer.valueOf(pathName.substring(12));
           	while (true) {
        		List<String> list = zk.getChildren(root, false);
        		Integer min = Integer.valueOf(list.get(0).substring(5));
        		System.out.println("List: "+list.toString());
        		String minString = list.get(0);
        		for(String s : list){
        			Integer tempValue = Integer.valueOf(s.substring(5));
        			if(tempValue < min)  {
        				min = tempValue;
        				minString = s;
        			}
        		}
        		System.out.println("Suffix: "+suffix+", min: "+min);
        		if (suffix.equals(min)) {
        			this.leader();
        			return true;
        		}
        		Integer max = min;
        		String maxString = minString;
        		for(String s : list){
        			Integer tempValue = Integer.valueOf(s.substring(5));
        			if(tempValue > max && tempValue < suffix)  {
        				max = tempValue;
        				maxString = s;
        			}
        		}
        		Stat s = zk.exists(root+"/"+maxString, this);
        		System.out.println("Watching "+root+"/"+maxString);
        		if (s != null) {
        			break;
        		}
        	}
        	System.out.println(pathName+" is waiting for a notification!");
        	return false;
        	
        }
        
        synchronized public void process(WatchedEvent event) {
            synchronized (mutex) {
                // This process is specific to Leader Election and would conflict if used
                // at the same time as the new Lock. For the worker, we rely on the
                // base class's process() method.
            	if (event.getType() == Event.EventType.NodeDeleted) {
            		try {
            			boolean success = check();
            			if (success) {
            				compute();
            			}
            		} catch (Exception e) {e.printStackTrace();}
            	}
            }
        }
        
        void leader() throws KeeperException, InterruptedException {
			System.out.println("Become a leader: "+id+"!");
            Stat s2 = zk.exists(leader, false);
            if (s2 == null) {
                zk.create(leader, id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
            	zk.setData(leader, id.getBytes(), 0);
            }
        }
        
        void compute() {
    		System.out.println("I will die after 10 seconds!");
    		try {
				new Thread().sleep(10000);    				
        		System.out.println("Process "+id+" died!");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		System.exit(0);
        }
    }
    
    public static void main(String args[]) {
        if (args.length == 0) {
            System.err.println("Por favor, especifique uma opcao: qTest, barrier, lock, leader, worker");
            return;
        }
        if (args[0].equals("qTest"))
            queueTest(args);
        else if (args[0].equals("barrier"))
            barrierTest(args);
        else if (args[0].equals("lock"))
        	lockTest(args);
		else if (args[0].equals("leader"))
			leaderElection(args);
        else if (args[0].equals("worker"))
            workerProcess(args);
        else
        	System.err.println("Opcao desconhecida: " + args[0]);
    }

    public static void queueTest(String args[]) {
        Queue q = new Queue(args[1], "/tickets");

        System.out.println("Input: " + args[1]);
        int i;
        Integer max = Integer.valueOf(args[2]);

        System.out.println("Producer");
		for (i = 0; i < max; i++)
			try{
				q.produce(10 + i);
			} catch (KeeperException | InterruptedException e){
				e.printStackTrace();
			}
    }

    public static void barrierTest(String args[]) {
        Barrier b = new Barrier(args[1], "/workers", Integer.valueOf(args[2]));
        try{
            boolean flag = b.enter();
            System.out.println("Entrou na barreira: " + args[2]);
            if(!flag) System.out.println("Erro ao entrar na barreira");
        } catch (KeeperException | InterruptedException e){
            e.printStackTrace();
        }

        // ... (original barrier test logic)
        System.out.println("Deixou a barreira");
    }
    
    public static void lockTest(String args[]) {
        // This test now uses the refactored Lock
    	Lock lock = new Lock(args[1],"/lock");
        long waitTime = Long.valueOf(args[2]);
        try{
            lock.lock();
            System.out.println("Lock adquirido no teste. Aguardando " + waitTime + "ms.");
            Thread.sleep(waitTime);
            lock.unlock();
            System.out.println("Lock liberado no teste.");
        } catch (KeeperException | InterruptedException e){
        	e.printStackTrace();
        }
    }
	
	public static void leaderElection(String args[]) {
        // Generate random integer
        Random rand = new Random();
        int r = rand.nextInt(1000000);
    	Leader leader = new Leader(args[1],"/election","/leader",r);
        try{
        	boolean success = leader.elect();
        	if (success) {
        		leader.compute();
        	} else {
        		while(true) {
        			//Waiting for a notification
        		}
            }         
        } catch (KeeperException | InterruptedException e){
        	e.printStackTrace();
        }
    }

    /**
     * Lógica principal do processo worker, com reeleição estável.
     */
    public static void workerProcess(String args[]) {
        if (args.length < 3) {
            System.err.println("USO: worker <endereco_zk> <tamanho_barreira>");
            return;
        }
        String zkAddress = args[1];
        int barrierSize = Integer.valueOf(args[2]);
        String barrierRoot = "/workers";
        String queueRoot = "/tickets";
        String lockRoot = "/work-lock";
        String electionRoot = "/election";
        String workerId = "worker-" + new Random().nextInt(100000);
        System.out.println("Worker " + workerId + " iniciando...");

        // Conexão e criação dos nós pais
        new SyncPrimitive(zkAddress);
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

        // 1. Barreira de Sincronização
        Barrier b = new Barrier(zkAddress, barrierRoot, barrierSize);
        try {
            System.out.println(workerId + " esta esperando na barreira " + barrierRoot);
            b.enter();
            System.out.println(workerId + " passou da barreira. Pronto para trabalhar.");
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return;
        }

        // 2. Entra na eleição APENAS UMA VEZ para obter sua identidade
        String myNodePath;
        try {
            myNodePath = createElectionNode(electionRoot, workerId);
            System.out.println(workerId + " entrou na eleicao com o no: " + myNodePath.substring(electionRoot.length() + 1));
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Worker " + workerId + " nao conseguiu entrar na eleicao.");
            e.printStackTrace();
            return;
        }


        // 3. Loop principal de verificação de liderança
        while (true) {
            try {
                // Com a identidade já estabelecida, apenas verifica se é o líder
                if (isLeader(electionRoot, myNodePath)) {
                    // *** LÓGICA DO LÍDER ***
                    System.out.println(">>>> " + workerId + " assumiu a LIDERANCA! <<<<");
                    leaderLoop(new Queue(zkAddress, queueRoot), new Lock(zkAddress, lockRoot), workerId);
                    // O líder fica preso em `leaderLoop` até cair. Se ele sair
                    // daqui por uma exceção não fatal, algo está errado, então saímos.
                    break;
                } else {
                    // *** LÓGICA DO SEGUIDOR ***
                    // Vigia o predecessor. Bloqueia até que ele caia.
                    watchPredecessor(electionRoot, myNodePath);
                    // Ao acordar, o loop recomeça para reavaliar a liderança
                    // com o MESMO `myNodePath`.
                    System.out.println(workerId + ": O lider/predecessor caiu. Tentando nova eleicao...");
                }
            } catch (KeeperException.SessionExpiredException e) {
                System.err.println("Sessao com Zookeeper expirada. Encerrando.");
                return;
            } catch (KeeperException | InterruptedException e) {
                System.err.println("Worker " + workerId + " encontrou um erro. Reiniciando checagem em 5s.");
                e.printStackTrace();
                try { Thread.sleep(5000); } catch (InterruptedException ie) {}
            }
        }
    }

    /**
     * O loop de trabalho exclusivo do líder.
     */
    public static void leaderLoop(Queue ticketQueue, Lock workLock, String workerId) throws KeeperException, InterruptedException {
        while (true) {
            String ticket = ticketQueue.consume();
            System.out.println("LIDER (" + workerId + ") pegou o ticket: " + ticket);

            workLock.lock();
            System.out.println("LIDER (" + workerId + ") esta processando o ticket (trabalho de 10s)...");
            Thread.sleep(10000);
            System.out.println(">>>> LIDER (" + workerId + "): " + ticket + " respondido <<<<");
            workLock.unlock();
        }
    }

    /**
     * Cria o nó para participar da eleição.
     */
    public static String createElectionNode(String electionRoot, String workerId) throws KeeperException, InterruptedException {
        return zk.create(electionRoot + "/node-", workerId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    /**
     * Verifica se o nó atual é o líder (o primeiro da lista).
     */
    public static boolean isLeader(String electionRoot, String myNodePath) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(electionRoot, false);
        Collections.sort(children);
        String myNodeName = myNodePath.substring(electionRoot.length() + 1);
        return children.get(0).equals(myNodeName);
    }

    /**
     * Coloca uma vigilância no nó predecessor e bloqueia até que ele seja deletado.
     */
    public static void watchPredecessor(String electionRoot, String myNodePath) throws KeeperException, InterruptedException {
        String myNodeName = myNodePath.substring(electionRoot.length() + 1);
        
        // Loop para garantir que estamos vigiando o nó correto
        while(true) {
            List<String> children = zk.getChildren(electionRoot, false);
            Collections.sort(children);
            int myIndex = children.indexOf(myNodeName);

            // Se meu nó desapareceu por algum motivo, sai para o loop principal tentar de novo
            if (myIndex == -1) {
                 System.err.println("Nao encontrei meu no ("+myNodeName+"). Saindo da vigilia.");
                 return;
            }
            // Se eu sou o líder agora, não preciso vigiar ninguém
            if (myIndex == 0) {
                return;
            }

            String predecessorName = children.get(myIndex - 1);
            
            synchronized (mutex) {
                Stat stat = zk.exists(electionRoot + "/" + predecessorName, true);
                if (stat != null) {
                    System.out.println("Seguidor " + myNodeName + " esta vigiando " + predecessorName);
                    mutex.wait(); // Espera a notificação
                    return; // Retorna ao ser notificado
                }
                // Se o stat for nulo, o predecessor caiu antes de conseguirmos vigiá-lo.
                // O loop while(true) interno tentará novamente.
            }
        }
    }
}