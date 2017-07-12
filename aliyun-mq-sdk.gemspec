# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'aliyun/mq/sdk/version'

Gem::Specification.new do |spec|
  spec.name          = "aliyun-mq-sdk"
  spec.version       = Aliyun::Mq::Sdk::VERSION
  spec.authors       = ["Jim"]
  spec.email         = ["jim.jin2006@gmail.com"]

  spec.summary       = %q{Aliyun MQ SDK for ruby}
  spec.description   = %q{Aliyun MQ actions, send order message, receive, delete.}
  spec.homepage      = "http://agideo.com"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "https://rubygems.org"
  else
    raise "RubyGems 2.0 or newer is required to protect against public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "httparty", "~> 0.15.5"
  spec.add_dependency "stomp"
  spec.add_dependency "json"

  spec.add_development_dependency "bundler", "~> 1.12"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "pry", "~> 0.10.4"
  spec.add_development_dependency "pry-byebug"
end
