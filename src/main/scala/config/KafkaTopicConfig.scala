package config

class KafkaTopicConfig extends GenericConfig {
  var process: String = _
  var source_topic: String = _
  var commit_offset: String = _
  var created_at: String = _
}
