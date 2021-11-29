with import <nixpkgs> {};
stdenv.mkDerivation {
  name = "tum-clt-svr";
  version = "0.1.0";
  
  src = ./.;
  
  buildInputs = [
    codespell
    (clang-tools.override { llvmPackages = llvmPackages_12; })
    cppcheck
    bashInteractive
    cmake
    protobuf
    ninja
    fmt
    python3
  ];

  configurePhase = ''
    cmake -S . -B build/release -D CMAKE_BUILD_TYPE=Release -GNinja
  '';

  buildPhase = ''
    cmake --build build/release
  '';

  installPhase = ''
    cmake --install build/release --prefix $out/bin
  '';
}
