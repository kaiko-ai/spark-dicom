let
  sources = import ./nix/sources.nix;
  sbt-derivation = import sources.sbt-derivation;
  pkgs = import sources.nixpkgs { overlays = [ sbt-derivation ]; };
  inherit (import sources."gitignore.nix" { inherit (pkgs) lib; }) gitignoreSource;
in
pkgs.sbt.mkDerivation {
  pname = "spark-dicom";
  version = "0.1";

  # see https://github.com/zaninime/sbt-derivation
  # basically, when changing sbt dependencies:
  # 1. reset this to "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
  # 2. let Nix compute hash
  # 3. use computed hash here and re-run build
  #
  # Sometimes, the sha256 computed locally is not the same as the one in CI.
  # In such cases, believe in the CI, it's most likely the right one.
  # You can run the CI locally using `act` provided in the Nix shell
  depsSha256 = "ce99OF853Z3IyP2+KEeBcv84HbuLVxkT9OFVwoVtf7A=";


  src = gitignoreSource ./.;

  buildPhase = ''
    sbt compile
    sbt package
  '';

  doCheck = true;
  checkPhase = ''
    sbt test
  '';

  installPhase = ''
    mkdir -p build/bin/jars/
    cp target/scala-*/spark-dicom*.jar build/bin/jars/
    cp -r build/bin/ $out/
  '';
}
