echo Recompilando projecto aguarde...
@echo off
mvn clean compile
mvn clean install -U
mvn exec:java