package io.cdap.cdap.app.etl.batch;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.etl.DataprocETLTestBase;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.Tasks;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class GoogleBigQueryTest extends DataprocETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleBigQueryTest.class);
  private static final String BIG_QUERY_PLUGIN_NAME = "BigQueryTable";
  private static final String SOURCE_TABLE_NAME_TEMPLATE = "test_source_table_";
  private static final String SINK_TABLE_NAME_TEMPLATE = "test_sink_table_";

  private static final Map<String, Object> SIMPLE_SOURCE = new ImmutableMap.Builder<String, Object>()
    .put("string_value", "string_1")
    .put("int_value", 1)
    .put("float_value", 0.1f)
    .put("boolean_value", true)
    .build();

  private static final Map<String, Object> UPDATE_SOURCE = new ImmutableMap.Builder<String, Object>()
    .put("string_value", "string_1")
    .put("int_value", 1)
    .put("float_value", 0.1f)
    .put("boolean_value", true)
    .put("numeric_value", 123.456)
    .put("timestamp_value", "2014-08-01 12:41:35.220000+00:00")
    .put("date_value", "2014-08-01")
    .build();

  private static final Map<String, Object> FULL_SOURCE = new ImmutableMap.Builder<String, Object>()
    .put("string_value", "string_1")
    .put("int_value", 1)
    .put("float_value", 0.1f)
    .put("boolean_value", true)
    .put("numeric_value", 123.456)
    .put("timestamp_value", "2014-08-01 12:41:35.220000+00:00")
    .put("date_value", "2014-08-01")
    .put("time_value", "01:41:35.220000")
    .put("datetime_value", "2014-08-01 01:41:35.220000")
    .put("string_array", Arrays.asList("a_string_1", "a_string_2"))
    .build();

  private static String bigQueryDataset;
  private static Dataset dataset;
  private static BigQuery bq;

  @BeforeClass
  public static void testClassSetup() throws IOException {
    UUID uuid = UUID.randomUUID();
    bigQueryDataset = "bq_dataset_" + uuid.toString().replaceAll("-", "_");
    try (InputStream inputStream = new ByteArrayInputStream(
      getServiceAccountCredentials().getBytes(StandardCharsets.UTF_8))) {
      bq = BigQueryOptions.newBuilder()
        .setProjectId(getProjectId())
        .setCredentials(GoogleCredentials.fromStream(inputStream))
        .build()
        .getService();
    }
    createDataset();
  }

  @AfterClass
  public static void testClassClear() {
    deleteDatasets();
  }

  @Override
  protected void innerSetup() throws Exception {
    Tasks.waitFor(true, () -> {
      try {
        final ArtifactId dataPipelineId = TEST_NAMESPACE.artifact("cdap-data-pipeline", version);
        if (!bigQueryPluginExists(dataPipelineId, BatchSource.PLUGIN_TYPE)) {
          return false;
        }
        return bigQueryPluginExists(dataPipelineId, BatchSink.PLUGIN_TYPE);
      } catch (ArtifactNotFoundException e) {
        return false;
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
  }

  @Override
  protected void innerTearDown() throws Exception {
  }

  @Test
  public void testReadDataAndStoreInNewTable() throws Exception {
    final int testId = 1;

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, getSimpleFieldsSchema());
    insertData(bigQueryDataset, sourceTableName, SIMPLE_SOURCE);

    Assert.assertFalse(exists(destinationTableName));

    io.cdap.cdap.api.data.schema.Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    // insert data into new Kind within the same Namespace using Auto-generated key and custom indexes
    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 1;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-storeInNewTable");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    Assert.assertTrue(exists(destinationTableName));
    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName);
  }

  @Test
  public void testReadDataAndStoreInExistingTable() throws Exception {
    final int testId = 2;

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, getSimpleFieldsSchema());
    insertData(bigQueryDataset, sourceTableName, SIMPLE_SOURCE);

    createTestTable(bigQueryDataset, destinationTableName, getSimpleFieldsSchema());

    Assert.assertTrue(exists(destinationTableName));

    io.cdap.cdap.api.data.schema.Schema sourceSchema = getSimpleTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    // insert data into new Kind within the same Namespace using Auto-generated key and custom indexes
    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 1;

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-storeInExistingTable");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName);
  }

  @Test
  public void testReadDataAndStoreWithUpdateTableSchema() throws Exception {
    final int testId = 3;

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, getUpdatedFieldsSchema());
    insertData(bigQueryDataset, sourceTableName, UPDATE_SOURCE);

    createTestTable(bigQueryDataset, destinationTableName, getSimpleFieldsSchema());

    io.cdap.cdap.api.data.schema.Schema sourceSchema = getUpdatedTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    // insert data into new Kind within the same Namespace using Auto-generated key and custom indexes
    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("allowSchemaRelaxation", "true")
      .build();

    int expectedCount = 1;
    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-storeWithUpdateTableSchema");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    assertSchemaEquals(sourceTableName, destinationTableName);
    assertTableEquals(sourceTableName, destinationTableName);
  }

  @Test
  public void testProcessingAllBigQuerySupportTypes() throws Exception {
    final int testId = 4;

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    createTestTable(bigQueryDataset, sourceTableName, getFullFieldsSchema());
    insertData(bigQueryDataset, sourceTableName, FULL_SOURCE);

    io.cdap.cdap.api.data.schema.Schema sourceSchema = getFullTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("allowSchemaRelaxation", "false")
      .build();

    int expectedCount = 1;
    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-allBigQueryTypes");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    Assert.assertTrue(exists(destinationTableName));

    com.google.cloud.bigquery.Schema expectedSchema =  getTableSchema(TableId.of(bigQueryDataset, sourceTableName));
    com.google.cloud.bigquery.Schema actualSchema = getTableSchema(TableId.of(bigQueryDataset, destinationTableName));

    for (int i = 0; i < expectedSchema.getFields().size(); i++) {
      Field expectedSchemaField = expectedSchema.getFields().get(i);
      Field actualSchemaField = actualSchema.getFields().get(i);
      if (expectedSchemaField.getType() == LegacySQLTypeName.DATETIME) {
        // DATETIME type mapped to CDAP string
        Assert.assertEquals(expectedSchemaField.getName(), actualSchemaField.getName());
        Assert.assertEquals(expectedSchemaField.getMode(), actualSchemaField.getMode());
      } else {
        Assert.assertEquals(expectedSchemaField, actualSchemaField);
      }
    }

    assertTableEquals(sourceTableName, destinationTableName);
  }

  private void assertSchemaEquals(String expectedTableName, String actualTableName) {
    TableId expectedTableId = TableId.of(bigQueryDataset, expectedTableName);
    TableId actualTableId = TableId.of(bigQueryDataset, actualTableName);
    com.google.cloud.bigquery.Schema expectedSchema =  getTableSchema(expectedTableId);
    com.google.cloud.bigquery.Schema actualSchema = getTableSchema(actualTableId);
    Assert.assertEquals(expectedSchema, actualSchema);
  }

  private void assertTableEquals(String expectedTableName, String actualTableName) {
    TableId expectedTableId = TableId.of(bigQueryDataset, expectedTableName);
    TableId actualTableId = TableId.of(bigQueryDataset, actualTableName);
    com.google.cloud.bigquery.Schema expectedSchema =  getTableSchema(expectedTableId);
    com.google.cloud.bigquery.Schema actualSchema = getTableSchema(actualTableId);
    List<FieldValueList> expectedResult = getResultTableData(expectedTableId, expectedSchema);
    List<FieldValueList> actualResult = getResultTableData(actualTableId, actualSchema);
    Assert.assertEquals(expectedResult.size(), actualResult.size());

    for (int i = 0; i < expectedResult.size(); i++) {
      FieldValueList expectedFieldValueList = expectedResult.get(i);
      FieldValueList actualFieldValueList = actualResult.get(i);
      Objects.requireNonNull(expectedSchema).getFields().stream()
        .map(Field::getName)
        .forEach(fieldName -> {
          FieldValue expected = expectedFieldValueList.get(fieldName);
          FieldValue actual = actualFieldValueList.get(fieldName);
          String message = String.format("Values differ for field '%s'. Expected '%s' but was '%s'.", fieldName,
                                         expected, actual);
          Assert.assertEquals(message, expected, actual);
        });
    }
  }

  private boolean bigQueryPluginExists(ArtifactId dataPipelineId, String pluginType) throws Exception {
    return artifactClient.getPluginSummaries(dataPipelineId, pluginType, ArtifactScope.SYSTEM).stream()
      .anyMatch(pluginSummary -> BIG_QUERY_PLUGIN_NAME.equals(pluginSummary.getName()));
  }

  private GoogleBigQueryTest.DeploymentDetails deployApplication(Map<String, String> sourceProperties,
                                                                       Map<String, String> sinkProperties,
                                                                       String applicationName) throws Exception {

    ArtifactSelectorConfig artifact =  new ArtifactSelectorConfig("SYSTEM", "google-cloud", "[0.0.0, 100.0.0)");
    ETLStage source = new ETLStage("BigQuerySourceStage",
                                   new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, sourceProperties,
                                                 artifact));
    ETLStage sink = new ETLStage("BigQuerySinkStage",
                                 new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, sinkProperties,
                                               artifact));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app(applicationName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);
    return new DeploymentDetails(source, sink, appId, applicationManager);
  }

  private static void createDataset() {
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(bigQueryDataset).build();
    LOG.info("Creating dataset {}", bigQueryDataset);
    dataset = bq.create(datasetInfo);
    LOG.info("Created dataset {}", bigQueryDataset);
  }

  private com.google.cloud.bigquery.Schema getTableSchema(TableId tableId) {
    return bq.getTable(tableId).getDefinition().getSchema();
  }

  private static List<FieldValueList> getResultTableData(TableId tableId, com.google.cloud.bigquery.Schema schema) {
    TableResult tableResult = bq.listTableData(tableId, schema);
    List<FieldValueList> result = new ArrayList<>();
    tableResult.iterateAll().forEach(result::add);
    return result;
  }

  private static void deleteDatasets() {
    DatasetId datasetId = dataset.getDatasetId();
    LOG.info("Deleting dataset {}", bigQueryDataset);
    boolean deleted = bq.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
    if (deleted) {
      LOG.info("Deleted dataset {}", bigQueryDataset);
    }
  }

  private boolean exists(String tableId) {
    return dataset.get(tableId) != null;
  }

  private Table createTestTable(String datasetId, String tableId, Field[] fieldsSchema) {
    TableId table = TableId.of(datasetId, tableId);

    com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(fieldsSchema);
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    TableInfo tableInfo = TableInfo.newBuilder(table, tableDefinition).build();

    return bq.create(tableInfo);
  }

  private void insertData(String datasetId, String tableId, Map<String, ?> content) {
    TableId table = TableId.of(datasetId, tableId);
    InsertAllRequest req = InsertAllRequest.newBuilder(table)
      .addRow(content)
      .build();

    InsertAllResponse insertAllResponse = bq.insertAll(req);
    if (insertAllResponse.hasErrors()) {
      throw new RuntimeException(String.format("Unable to insert data: '%s'", insertAllResponse.getInsertErrors()));
    }
  }

  private Field[] getSimpleFieldsSchema() {
    return new Field[]{
      Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("float_value", LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    };
  }

  private Field[] getUpdatedFieldsSchema() {
    return new Field[]{
      Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("float_value", LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("numeric_value", LegacySQLTypeName.NUMERIC).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("timestamp_value", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("date_value", LegacySQLTypeName.DATE).setMode(Field.Mode.NULLABLE).build()
    };
  }

  private Field[] getFullFieldsSchema() {
    return new Field[] {
      Field.newBuilder("string_value", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("int_value", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("float_value", LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("numeric_value", LegacySQLTypeName.NUMERIC).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("boolean_value", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("timestamp_value", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("date_value", LegacySQLTypeName.DATE).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("time_value", LegacySQLTypeName.TIME).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("datetime_value", LegacySQLTypeName.DATETIME).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("string_array", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build()
    };
  }

  private Schema getSimpleTableSchema() {
    return Schema
      .recordOf("simpleTableSchema",
                Schema.Field.of("string_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                Schema.Field.of("float_value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                Schema.Field.of("boolean_value", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))
      );
  }

  private Schema getUpdatedTableSchema() {
    return Schema
      .recordOf("simpleTableSchema",
                Schema.Field.of("string_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                Schema.Field.of("float_value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                Schema.Field.of("boolean_value", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                Schema.Field.of("numeric_value", Schema.nullableOf(Schema.decimalOf(38, 9))),
                Schema.Field.of("timestamp_value", Schema.nullableOf(Schema.of(
                  Schema.LogicalType.TIMESTAMP_MICROS))),
                Schema.Field.of("date_value", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE)))
      );
  }

  private Schema getFullTableSchema() {
    return Schema
      .recordOf("bigQuerySourceSchema",
                Schema.Field.of("string_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                Schema.Field.of("float_value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                Schema.Field.of("numeric_value", Schema.nullableOf(Schema.decimalOf(38, 9))),
                Schema.Field.of("boolean_value", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                Schema.Field.of("timestamp_value", Schema.nullableOf(Schema.of(
                  Schema.LogicalType.TIMESTAMP_MICROS))),
                Schema.Field.of("date_value", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                Schema.Field.of("time_value", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS))),
                Schema.Field.of("datetime_value", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                Schema.Field.of("string_array", Schema.arrayOf(Schema.of(Schema.Type.STRING)))
      );
  }

  private class DeploymentDetails {

    private final ApplicationId appId;
    private final ETLStage source;
    private final ETLStage sink;
    private final ApplicationManager appManager;

    DeploymentDetails(ETLStage source, ETLStage sink, ApplicationId appId, ApplicationManager appManager) {
      this.appId = appId;
      this.source = source;
      this.sink = sink;
      this.appManager = appManager;
    }

    public ApplicationId getAppId() {
      return appId;
    }

    public ETLStage getSource() {
      return source;
    }

    public ETLStage getSink() {
      return sink;
    }

    public ApplicationManager getAppManager() {
      return appManager;
    }
  }

}
