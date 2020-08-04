require "json"

package = JSON.parse(File.read(File.join(__dir__, "package.json")))

Pod::Spec.new do |s|
  s.name         = "react-native-leveldown"
  s.version      = package["version"]
  s.summary      = package["description"]
  s.description  = <<-DESC
                  react-native-leveldown
                   DESC
  s.homepage     = "https://github.com/github_account/react-native-leveldown"
  s.license      = "MIT"
  s.authors      = { "Andy Matuschak" => "andy@andymatuschak.org" }
  s.platforms    = { :ios => "9.0" }
  s.source       = { :git => "https://github.com/andymatuschak/react-native-leveldown.git", :tag => "#{s.version}" }

  s.source_files = "ios/**/*.{h,m,mm,swift}"
  s.requires_arc = true

  s.dependency "React"
  s.dependency "leveldb-library"
  # ...
  # s.dependency "..."
end

