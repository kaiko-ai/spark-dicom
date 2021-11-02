let
  sources = import ./nix/sources.nix;
  sbt-derivation = import sources.sbt-derivation;
  pkgs = import sources.nixpkgs { overlays = [ sbt-derivation ]; };
in
pkgs.sbt.mkDerivation {
  pname = "spark-dicom";
  version = "0.1";

  # see https://github.com/zaninime/sbt-derivation
  # basically, when changing dependencies:
  # 1. reset this to 0
  # 2. let Nix compute hash
  # 3. use computed hash here and re-run build
  depsSha256 = "9qea6+Ui7NdZ1f3iwRp1x0w0P7KJp4eQACaB/5vyYX8=";

  src = ./.;

  buildPhase = ''
    sbt package
  '';

  installPhase = ''
    mkdir -p build/bin/
    cp target/scala-*/spark-dicom*.jar build/bin/
    cp -r build/bin/ $out
  '';
}
