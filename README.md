# signature-commons-controller

Preparing and controlling signature commons with existing data / store.

This repository include scripts to facilitate data ingestion from different forms to those which can be used by the signature commons. It can subsequently upload the processed data to the signature commons through the relevant APIs.

## Installation
```bash
# Install package from github repository master
pip install --upgrade git+git://github.com/maayanlab/signature-commons-controller.git
```

## Usage
```bash
sigcom --help
```

## Development

### Build SignatureCommonsDataIngestion.jar
```bash
javac SignatureCommonsDataIngestion.java
jar cvfe SignatureCommonsDataIngestion.jar SignatureCommonsDataIngestion SignatureCommonsDataIngestion.class
```

### A note about matrix orientation
The standard orientation of a `.data.tsv` matrix looks like so:

|      |Signature 1|Signature 2|...|
|------|-----------|-----------|---|
|Gene A|           |           |   |
|Gene B|           |           |   |
| ...  |           |           |   |
