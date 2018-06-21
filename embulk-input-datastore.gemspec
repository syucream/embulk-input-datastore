Gem::Specification.new do |spec|
  spec.name          = "embulk-input-datastore"
  spec.version       = "0.0.2"
  spec.authors       = ["syucream"]
  spec.summary       = %[Datastore input plugin for Embulk]
  spec.description   = %[Loads records from datastore.]
  spec.email         = ["syucream1031@gmail.com"]
  spec.licenses      = ["MIT"]
  spec.homepage      = "https://github.com/syucream/embulk-input-datastore"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end