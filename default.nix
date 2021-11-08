let
  sources = import ./nix/sources.nix;
  sbt-derivation = import sources.sbt-derivation;
  pkgs = import sources.nixpkgs { overlays = [ sbt-derivation ]; };
in
pkgs.sbt.mkDerivation {
  pname = "spark-dicom";
  version = "0.1";

  # see https://github.com/zaninime/sbt-derivation
  # basically, when changing sbt dependencies:
  # 1. reset this to "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
  # 2. let Nix compute hash
  # 3. use computed hash here and re-run build
  depsSha256 = "o9L1axgPcjuzDANgw1PmL4UK2/pffpO88p5YPlmlgIA=";

  src = ./.;

  buildPhase = ''
    sbt assembly
  '';

  doCheck = true;
  checkPhase = ''
    sbt test
  '';

  installPhase = ''
    mkdir -p build/bin/
    cp target/scala-*/spark-dicom*.jar build/bin/
    cp -r build/bin/ $out
  '';
}
