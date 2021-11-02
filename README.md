# spark-dicom

Spark DICOM connector in Scala

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

#### Warning

When changing sbt build dependencies, change `depsSha256` in `default.nix` as instructed.
