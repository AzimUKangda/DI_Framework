package config

class IntTestConfig extends GenericConfig {
  //common variable
  var jsonDataPath: String = _
  //Hub Variables
  var hubExportCols: List[String] = _
  var hubTargetTable: String = _
  var hubDmlFilePath: String = _
  var hubOutputTestFilePath: String = _
  var hubOutputBaselineTestFilePath: String = _
  var hubtwoExportCols: List[String] = _
  var hubtwoTargetTable: String = _
  var hubtwoDmlFilePath: String = _
  var hubtwoOutputTestFilePath: String = _
  var hubtwoOutputBaselineTestFilPath: String = _
  //Sat Variables
  var satExportCols: List[String] = _
  var satTargetTable: String = _
  var satDmlFilePath: String = _
  var satOutputTestFilePath: String = _
  var satOutputBaselineTestFilePath: String = _
  //Link Variables
  var linkExportCols: List[String] = _
  var linkTargetTable: String = _
  var linkDmlFilePath: String = _
  var linkOutputTestFilePath: String = _
  var linkOutputBaselineTestFilePath: String = _


}
