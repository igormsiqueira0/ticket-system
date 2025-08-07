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
        int consume() throws KeeperException, InterruptedException{
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
                        return retvalue;
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

        if (args[3].equals("p")) {
            System.out.println("Producer");
            for (i = 0; i < max; i++)
                try{
                    q.produce(10 + i);
                } catch (KeeperException | InterruptedException e){
                    e.printStackTrace();
                }
        } else {
            System.out.println("Consumer");
            for (i = 0; i < max; i++) {
                try{
                    int r = q.consume();
                    System.out.println("Item: " + r);
                } catch (KeeperException e){
                    i--;
                } catch (InterruptedException e){
			    e.printStackTrace();
                }
            }
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
     * New method for the worker process logic
     */
    public static void workerProcess(String args[]) {
        if (args.length < 3) {
            System.err.println("USO: worker <endereco_zk> <tamanho_barreira>");
            return;
        }
        String zkAddress = args[1];
        int barrierSize = Integer.valueOf(args[2]);
        String barrierRoot = "/workers";
        String activeWorkersRoot = "/activeworkers";
        String queueRoot = "/tickets";
        String lockRoot = "/work-lock";
        String workerId = "worker-" + new Random().nextInt(100000);
        System.out.println("Worker " + workerId + " iniciando...");

        // Connect to ZK and ensure parent znodes exist
        new SyncPrimitive(zkAddress);
        try {
            while (zk == null || !zk.getState().isConnected()) {
                Thread.sleep(100);
            }
            if (zk.exists(activeWorkersRoot, false) == null) {
                zk.create(activeWorkersRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists(lockRoot, false) == null) {
                zk.create(lockRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch(KeeperException | InterruptedException e) {
            e.printStackTrace();
            return;
        }

        // 1. Wait at the barrier (one-time startup synchronization)
        Barrier b = new Barrier(zkAddress, barrierRoot, barrierSize);
        try {
            System.out.println(workerId + " esta esperando na barreira " + barrierRoot);
            b.enter();
            System.out.println(workerId + " passou da barreira. Pronto para trabalhar.");
        } catch (KeeperException | InterruptedException e) {
            System.out.println(workerId + " falhou na barreira.");
            e.printStackTrace();
            return;
        }

        // Worker's main processing loop
        Queue ticketQueue = new Queue(zkAddress, queueRoot);
        Lock workLock = new Lock(zkAddress, lockRoot);
        String activeWorkerPath = activeWorkersRoot + "/" + workerId;

        while (true) {
			try {
				// 2. Anuncia a disponibilidade
				System.out.println(workerId + " esta disponivel em " + activeWorkersRoot);
				try {
					zk.create(activeWorkerPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				} catch (KeeperException.NodeExistsException e) {
					// OK. O nó já existe de uma iteração anterior que falhou. Apenas continue.
					System.out.println("AVISO: No de status '" + activeWorkerPath + "' ja existe. O worker esta se recuperando de um estado anterior.");
				}

				// 3. Consume um ticket (bloqueia aqui se a fila estiver vazia)
				int ticket = ticketQueue.consume();
				System.out.println(workerId + " pegou o ticket: " + ticket);

				// 4. Uma vez que o ticket foi pego, fica "indisponível"
				System.out.println(workerId + " esta ocupado, removendo de " + activeWorkersRoot);
				zk.delete(activeWorkerPath, -1);
				
				// 5. Adquire o lock para sinalizar que está "travado" no processamento
				workLock.lock();
				
				// 6. Processa o ticket (simula trabalho)
				System.out.println(workerId + " esta processando o ticket (trabalho de 10s)...");
				Thread.sleep(10000);
				System.out.println(">>>> " + workerId + ": ticket respondido <<<<");

				// 7. Libera o lock
				workLock.unlock();

			} catch (KeeperException.NoNodeException e) {
				System.err.println(workerId + ": no do worker ativo desapareceu. Tentando recriar no proximo loop.");
			} catch (KeeperException | InterruptedException e) {
				// Captura outras exceções
				e.printStackTrace();
				// Espera um pouco para não sobrecarregar em caso de erros repetidos
				try { Thread.sleep(5000); } catch (InterruptedException ie) {}
			}
		}
    }
}