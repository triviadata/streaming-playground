## Avro

### Avro tools OSX
Are compiled using avro tools on osx to repeat the process run:
* brew install avro-tools
* cd core/src/main
* avro-tools compile -string schema resources java


### Avro tools as a dependency

* Download: avro-tools-1.8.1.jar
* cd core/src/main
* java -jar $path_to_avro_tools/avro-tools-1.8.1.jar compile -string schema resources java 
