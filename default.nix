let
  sources = import ./nix/sources.nix;
  sbt-derivation = import sources.sbt-derivation;
  pkgs = import sources.nixpkgs { overlays = [ sbt-derivation ]; };
  inherit (import sources."gitignore.nix" { inherit (pkgs) lib; }) gitignoreSource;
in
pkgs.sbt.mkDerivation {
  pname = "spark-dicom";
  version = builtins.elemAt (builtins.match "^.*[\"](.+)[\"].*$" (builtins.readFile ./version.sbt)) 0;

  # see https://github.com/zaninime/sbt-derivation
  # basically, when changing sbt dependencies:
  # 1. reset this to "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
  # 2. let Nix compute hash
  # 3. use computed hash here and re-run build
  #
  # Sometimes, the sha256 computed locally is not the same as the one in CI.
  # In such cases, believe in the CI, it's most likely the right one.
  # You can run the CI locally using `act -j build` provided in the Nix shell
  # depsSha256 = "9HIaLbvTLi/wrZGzlMe4lGTbLH2a56Qn2WDCfDExIls=";
  depsSha256 = "sha256-PNzwOb//bIVi0oJEEOpjuXj8LhZD1dVfL+mFvszhsmE=";

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
    cp target/spark-dicom*.jar build/bin/jars/
    cp -r build/bin/ $out/
  '';
}
