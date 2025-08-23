set ZK="C:\tmp\apache-zookeeper-3.9.3-bin"
set CP_ZK=.;%ZK%\lib\zookeeper-3.9.3.jar;%ZK%\lib\zookeeper-jute-3.9.3.jar;%ZK%\lib\slf4j-api-1.7.30.jar;%ZK%\lib\logback-core-1.2.13.jar;%ZK%\lib\logback-classic-1.2.13.jar;%ZK%\lib\netty-handler-4.1.113.Final.jar
javac -cp %CP_ZK% *.java
@echo ** Iniciando Corredor **
set RACERS=3
@echo Numero de corredores no grid de largada = %RACER_COUNT%
java -cp %CP_ZK% -Dlogback.configurationFile=file:%ZK%\conf\logback.xml LeMansRace racer localhost %RACERS%