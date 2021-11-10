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

> Note: when changing sbt build dependencies, change `depsSha256` in `default.nix` as instructed.

### CI

CI is handled by GitHub actions, using Nix for dependency management, test, build and caching (with Cachix).

> Note: for CI to run tests, the CI needs the Nix build to run tests in checkPhase.

You can run the CI locally using `act` (provided in the Nix shell).
