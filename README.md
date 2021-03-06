# spark-dicom

Spark DICOM connector in Scala

## How to use

Once loaded in the classpath of your Spark cluster, you can load DICOM data in Spark using the `dicomFile` as follows:

```scala
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

### Pixel Data

The `PixelData` attribute in a DICOM file can be very heavy and make Spark crash.
Reading it is disabled by default.
In order to be able to select the `PixelData` column, please turn the `includePixelData` option on:

```scala
spark.read.format("dicomFile").option("includePixelData", true).load("/some/hdfs/path").select("PixelData")
```

### Other columns

- `isDicom`: `true` if file was read as a DICOM file, `false` otherwise


## De-identification

The DICOM dataframe can be de-identified according to the Basic Confidentiality Profile in the [DICOM standard](https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E). To use the de-identifier, do the following in scala:

```scala
import ai.kaiko.spark.dicom.deidentifier.DicomDeidentifier._

var df = spark.read.format("dicomFile").load("/some/hdfs/path")
df = deidentify(df)
```

The resulting dataframe will have all the columns dropped/emptied/dummyfied according to the actions described [here](https://dicom.nema.org/medical/dicom/current/output/html/part15.html#table_E.1-1).

To perform the de-identification with any of the options described in the table, use:

```scala
import ai.kaiko.spark.dicom.deidentifier.DicomDeidentifier._
import ai.kaiko.spark.dicom.deidentifier.options._

val config: Map[DeidOption, Boolean] = Map(
  CleanDesc -> true,
  RetainUids -> true
)

var df = spark.read.format("dicomFile").load("/some/hdfs/path")
df = deidentify(df, config)
```

Current limitations of the de-identification are:
| Expected behavior                            | Current behavior                                         |
| -------------------------------------------- | -------------------------------------------------------- |
| Tags with `SQ` VR are de-identified          | Tags with `SQ` VR are ignored                            |
| Private tags are de-identified               | Private tags are ignored                                 |
| The `U` action pseudonimizes the value       | The `U` action replaces the value with `ToPseudonimize`  |
| The `C` action cleans the value of PHI/PII   | The `C` action replaces the value with `ToClean`         |

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

#### Updating dependencies

When changing sbt build dependencies, change `depsSha256` in `default.nix` as instructed.

### CI

CI is handled by GitHub actions, using Nix for dependency management, test, build and caching (with Cachix).

> Note: for CI to run tests, the CI needs the Nix build to run tests in checkPhase.

You can run the CI locally using `act` (provided in the Nix shell).

## Release

Creating a release is done with the help of the [sbt-sonatype](https://github.com/xerial/sbt-sonatype), [sbt-pgp](https://github.com/sbt/sbt-pgp) and [sbt-release](https://github.com/sbt/sbt-release) plugins.

Before starting, make sure to set the [Sonatype credentials](https://github.com/xerial/sbt-sonatype#homesbtsbt-version-013-or-10sonatypesbt) as environment variables: `SONATYPE_USERNAME` & `SONATYPE_PASSWORD`. In addition, make sure to have the `gpg` utility installed and the release GPG Key available in your keyring.

Then, run:

```
$ nix-shell
$ sbt
$ release
```

You will be prompted for the "release version", the "next version" and the GPG Key passphrase. Make sure to follow the [SemVer](https://www.scala-lang.org/blog/2021/02/16/preventing-version-conflicts-with-versionscheme.html) versioning scheme. If all went well, the new release should be available on [Maven Central](https://search.maven.org/artifact/ai.kaiko/spark-dicom) in 10 minutes.
