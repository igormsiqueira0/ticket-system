set ZK="C:\tmp\apache-zookeeper-3.9.3-bin"
set CP_ZK=.;%ZK%\lib\zookeeper-3.9.3.jar;%ZK%\lib\zookeeper-jute-3.9.3.jar;%ZK%\lib\slf4j-api-1.7.30.jar;%ZK%\lib\logback-core-1.2.13.jar;%ZK%\lib\logback-classic-1.2.13.jar;%ZK%\lib\netty-handler-4.1.113.Final.jar
javac -cp %CP_ZK% *.java
@echo ***** Cronômetro Le Mans - Produtor de Voltas *****
set TEMPOS=24
@echo Enviando %TOTAL_LAPS% voltas para a corrida
java -cp %CP_ZK% -Dlogback.configurationFile=file:%ZK%\conf\logback.xml LeMansRace lapProducer localhost %TEMPOS%