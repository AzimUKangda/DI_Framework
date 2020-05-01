package config

class MetadataSatConfig extends GenericConfig {
  var coreJobActive: String = _
  var sourceTopic: String = _
  var sourceColumns: String = _
  var businessKey: String = _
  var multiactivePart: String = _
  var hubHashkeyOrder: String = _
  var rawHiveDB: String = _
  var rawHdfsPath: String = _
  var satTable: String = _
  var satColumn: String = _
  var hubTable: String = _
  var hubSkColumn: String = _
  var targetFormat: String = _
  var targetPartitionColumn: String = _
  var requireBusinessKey: String = _
}
