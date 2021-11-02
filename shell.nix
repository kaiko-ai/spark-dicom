let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs { };
in
pkgs.mkShell {
  buildInputs = [
    # Nix tooling
    pkgs.nix
    pkgs.niv

    # JVM & Scala
    pkgs.jdk11
    pkgs.sbt
    pkgs.scalafmt
  ];
}
