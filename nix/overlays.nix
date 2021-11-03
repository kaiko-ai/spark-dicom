self: super: {
  metals =
    (super.metals.overrideAttrs
      (old: rec {
        deps = old.deps.overrideAttrs (oldDeps: { outputHash = "K/Pjo5VD9w4knelK0YwXFoZOzJDzjzlMjHF6fJZo524="; });
        buildInputs = [ deps super.jdk ];
      }));
}
