# signature-commons-controller

Preparing and controlling signature commons with existing data / store.

This repository include scripts to facilitate data ingestion from different forms to those which can be used by the signature commons. It can subsequently upload the processed data to the signature commons through the relevant APIs.

## Build SignatureCommonsDataIngestion.jar
```bash
javac SignatureCommonsDataIngestion.java
jar cvfe SignatureCommonsDataIngestion.jar SignatureCommonsDataIngestion SignatureCommonsDataIngestion.class
```
