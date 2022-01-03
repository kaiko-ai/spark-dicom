# spark-dicom

Spark DICOM connector in Scala

## How to use

Once loaded in the classpath of your Spark cluster, you can load DICOM data in Spark as follow:

```scala
val spark = SparkSession.builder.master("local").getOrCreate

val df = spark.read.format("dicomFile").load("/some/hdfs/path").select("PatientName", "StudyDate", "StudyTime")
```

You can select DICOM attributes defined in the DICOM standard registry using their keyword.
They are defined in the [official DICOM standard](https://dicom.nema.org/medical/dicom/2021d/output/chtml/part06/PS3.6.html).

Each attribute is written to a column with a Spark data type equivalent to its VR.
The mapping is as follows:

| VR                                                         | Spark Data type                                                   |
| ---------------------------------------------------------- | ----------------------------------------------------------------- |
| AE, AS, AT, CS, DS, DT, IS, LO, LT, SH, ST, UC, UI, UR, UT | String                                                            |
| PN                                                         | {"Alphabetic": String, "Ideographic": String, "Phonetic": String} |
| FL, FD                                                     | [Double]                                                          |
| SL, SS, US, UL                                             | [Integer]                                                         |
| SV, UV                                                     | [Long]                                                            |
| DA                                                         | String (formatted as `DateTimeFormatter.ISO_LOCAL_DATE`)          |
| TM                                                         | String (formatted as `DateTimeFormatter.ISO_LOCAL_TIME`)          |

## Development

### Development shell

A reproducible development environment is provided using [Nix](https://nixos.org/learn.html).

```
$ nix-shell
```

it will provide you the JDK, sbt, and all other required tools.

### Build with Nix

Build the JAR artifact:

```
$ nix-build
```

> Note: when changing sbt build dependencies, change `depsSha256` in `default.nix` as instructed.

### CI

CI is handled by GitHub actions, using Nix for dependency management, test, build and caching (with Cachix).

> Note: for CI to run tests, the CI needs the Nix build to run tests in checkPhase.

You can run the CI locally using `act` (provided in the Nix shell).
