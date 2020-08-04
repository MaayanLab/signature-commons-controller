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

But because genes are usually the common dimension, it is often more convenient and computationally efficient to store it in a transposed format `.data.T.tsv`

|           |Gene A|Gene B|...|
|-----------|------|------|---|
|Signature 1|      |      |   |
|Signature 2|      |      |   |
|   ...     |      |      |   |

In the first orientation, we are forced to load the entire matrix in memory before we know the common genes. The latter format allows us to parse the first row and then process each signature on a stream, freeing up space when we are done with it.

If you're running low on memory, keep this in mind transpose your data.
