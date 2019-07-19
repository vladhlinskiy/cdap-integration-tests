package io.cdap.cdap.app.etl.batch;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.etl.DataprocETLTestBase;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
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
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class GoogleBigQueryTest extends DataprocETLTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleBigQueryTest.class);
  private static final String BIG_QUERY_PLUGIN_NAME = "BigQueryTable";
  private static final String SOURCE_TABLE_NAME_TEMPLATE = "test_source_table_";
  private static final String SINK_TABLE_NAME_TEMPLATE = "test_sink_table_";
  private static final String SOURCE_PATH = "src" + System.getProperty("file.separator") + "test"
    + System.getProperty("file.separator") + "resources" + System.getProperty("file.separator");
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
  public void test_01_ReadDataAndStoreInNewTable() throws Exception {
    final int testId = 1;

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    Table sourceTable = createTestTable(bigQueryDataset, sourceTableName, getSimpleFieldsSchema());
    insertData(bigQueryDataset, sourceTableName, Paths.get(SOURCE_PATH + "simple_source.json"));

    Assert.assertFalse(isExisting(destinationTableName));

    io.cdap.cdap.api.data.schema.Schema sourceSchema = getSimpleTableSchem();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .put("serviceFilePath", getServiceAccountCredentials())
      .build();

    // insert data into new Kind within the same Namespace using Auto-generated key and custom indexes
    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("allowSchemaRelaxation", "false")
      .put("serviceFilePath", getServiceAccountCredentials())
      .build();

    int expectedCount = 1;

    List<FieldValue> expectedResult = getResultTableDate(TableId.of(bigQueryDataset, sourceTableName));
    com.google.cloud.bigquery.Schema expectedSchema = sourceTable.getDefinition().getSchema();

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-storeInNewTable");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    Assert.assertTrue(isExisting(destinationTableName));

    com.google.cloud.bigquery.Schema resultSchema = getTableSchema(TableId.of(bigQueryDataset, destinationTableName));
    Assert.assertEquals(expectedSchema, resultSchema);

    List<FieldValue> result = getResultTableDate(TableId.of(bigQueryDataset, destinationTableName));
    Assert.assertEquals(expectedResult.size(), result.size());

    for (int i = 0; i < result.size(); i++) {
      Assert.assertEquals(expectedResult.get(i).getAttribute(), result.get(i).getAttribute());
      Assert.assertEquals(expectedResult.get(i).getValue(), result.get(i).getValue());
    }
  }

  @Test
  public void test_02_ReadDataAndStoreInExistTable() throws Exception {
    final int testId = 2;

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    Table sourceTable = createTestTable(bigQueryDataset, sourceTableName, getSimpleFieldsSchema());
    insertData(bigQueryDataset, sourceTableName, Paths.get(SOURCE_PATH + "simple_source.json"));

    createTestTable(bigQueryDataset, destinationTableName, getSimpleFieldsSchema());

    Assert.assertTrue(isExisting(destinationTableName));

    io.cdap.cdap.api.data.schema.Schema sourceSchema = getSimpleTableSchem();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .put("serviceFilePath", getServiceAccountCredentials())
      .build();

    // insert data into new Kind within the same Namespace using Auto-generated key and custom indexes
    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("allowSchemaRelaxation", "false")
      .put("serviceFilePath", getServiceAccountCredentials())
      .build();

    int expectedCount = 1;

    List<FieldValue> expectedResult = getResultTableDate(TableId.of(bigQueryDataset, sourceTableName));
    com.google.cloud.bigquery.Schema expectedSchema = sourceTable.getDefinition().getSchema();

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-storeInExistTable");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    com.google.cloud.bigquery.Schema resultSchema = getTableSchema(TableId.of(bigQueryDataset, destinationTableName));
    Assert.assertEquals(expectedSchema, resultSchema);

    List<FieldValue> result = getResultTableDate(TableId.of(bigQueryDataset, destinationTableName));
    Assert.assertEquals(expectedResult.size(), result.size());

    for (int i = 0; i < result.size(); i++) {
      Assert.assertEquals(expectedResult.get(i).getAttribute(), result.get(i).getAttribute());
      Assert.assertEquals(expectedResult.get(i).getValue(), result.get(i).getValue());
    }
  }

  @Test
  public void test_03_ReadDataAndStoreWithUpdateTableSchema() throws Exception {
    final int testId = 3;

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    Table sourceTable = createTestTable(bigQueryDataset, sourceTableName, getUpdatedFieldsSchema());
    insertData(bigQueryDataset, sourceTableName, Paths.get(SOURCE_PATH + "update_source.json"));

    createTestTable(bigQueryDataset, destinationTableName, getSimpleFieldsSchema());

    io.cdap.cdap.api.data.schema.Schema sourceSchema = getUpdatedTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .put("serviceFilePath", getServiceAccountCredentials())
      .build();

    // insert data into new Kind within the same Namespace using Auto-generated key and custom indexes
    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("allowSchemaRelaxation", "true")
      .put("serviceFilePath", getServiceAccountCredentials())
      .build();

    int expectedCount = 1;

    List<FieldValue> expectedResult = getResultTableDate(TableId.of(bigQueryDataset, sourceTableName));
    com.google.cloud.bigquery.Schema expectedSchema = sourceTable.getDefinition().getSchema();

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-storeWithUpdateTableSchema");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    com.google.cloud.bigquery.Schema resultSchema = getTableSchema(TableId.of(bigQueryDataset, destinationTableName));
    Assert.assertEquals(expectedSchema, resultSchema);

    List<FieldValue> result = getResultTableDate(TableId.of(bigQueryDataset, destinationTableName));
    Assert.assertEquals(expectedResult.size(), result.size());

    for (int i = 0; i < result.size(); i++) {
      Assert.assertEquals(expectedResult.get(i).getAttribute(), result.get(i).getAttribute());
      Assert.assertEquals(expectedResult.get(i).getValue(), result.get(i).getValue());
    }
  }

  @Test
  public void test_04_ProcessingAllBigQuerySupportTypes() throws Exception {
    final int testId = 4;

    String sourceTableName = SOURCE_TABLE_NAME_TEMPLATE + testId;
    String destinationTableName = SINK_TABLE_NAME_TEMPLATE + testId;

    Table sourceTable = createTestTable(bigQueryDataset, sourceTableName, getFullFieldsSchema());
    insertData(bigQueryDataset, sourceTableName, Paths.get(SOURCE_PATH + "full_source.json"));

    io.cdap.cdap.api.data.schema.Schema sourceSchema = getFullTableSchema();

    Map<String, String> sourceProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_source")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", sourceTableName)
      .put("schema", sourceSchema.toString())
      .put("serviceFilePath", getServiceAccountCredentials())
      .build();

    Map<String, String> sinkProps = new ImmutableMap.Builder<String, String>()
      .put("referenceName", "bigQuery_sink")
      .put("project", getProjectId())
      .put("dataset", bigQueryDataset)
      .put("table", destinationTableName)
      .put("allowSchemaRelaxation", "false")
      .put("serviceFilePath", getServiceAccountCredentials())
      .build();

    int expectedCount = 1;

    List<FieldValue> expectedResult = getResultTableDate(TableId.of(bigQueryDataset, sourceTableName));
    com.google.cloud.bigquery.Schema expectedSchema = sourceTable.getDefinition().getSchema();

    GoogleBigQueryTest.DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, BIG_QUERY_PLUGIN_NAME + "-allBigQueryTypes");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());

    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out", expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in", expectedCount, 10);

    com.google.cloud.bigquery.Schema resultSchema = getTableSchema(TableId.of(bigQueryDataset, destinationTableName));
    Assert.assertEquals(expectedSchema, resultSchema);

    List<FieldValue> result = getResultTableDate(TableId.of(bigQueryDataset, destinationTableName));
    Assert.assertEquals(expectedResult.size(), result.size());

    for (int i = 0; i < result.size(); i++) {
      Assert.assertEquals(expectedResult.get(i).getAttribute(), result.get(i).getAttribute());
      Assert.assertEquals(expectedResult.get(i).getValue(), result.get(i).getValue());
    }
  }

  private boolean bigQueryPluginExists(ArtifactId dataPipelineId, String pluginType) throws Exception {
    return artifactClient.getPluginSummaries(dataPipelineId, pluginType, ArtifactScope.SYSTEM).stream()
      .anyMatch(pluginSummary -> BIG_QUERY_PLUGIN_NAME.equals(pluginSummary.getName()));
  }

  private GoogleBigQueryTest.DeploymentDetails deployApplication(Map<String, String> sourceProperties,
                                                                       Map<String, String> sinkProperties,
                                                                       String applicationName) throws Exception {
    ETLStage source = new ETLStage("BigQuerySourceStage",
                                   new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, sourceProperties));
    ETLStage sink = new ETLStage("BigQuerySinkStage",
                                 new ETLPlugin(BIG_QUERY_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, sinkProperties));

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

  private static List<FieldValue> getResultTableDate(TableId tableId) {
    TableResult tableResult = bq.listTableData(tableId);
    List<FieldValue> result = new ArrayList<>();
    tableResult.iterateAll().forEach(result::addAll);
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

  private boolean isExisting(String tableId) {
    return dataset.get(tableId) != null;
  }

  private Table createTestTable(String datasetId, String tableId, Field[] fieldsSchema) {
    TableId table = TableId.of(datasetId, tableId);

    com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(fieldsSchema);
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    TableInfo tableInfo = TableInfo.newBuilder(table, tableDefinition).build();

    return bq.create(tableInfo);
  }

  private void insertData(String datasetId, String tableId, Path jsonPath)
    throws IOException, InterruptedException {
    TableId table = TableId.of(datasetId, tableId);

    WriteChannelConfiguration writeChannelConfiguration =
      WriteChannelConfiguration.newBuilder(table).setFormatOptions(FormatOptions.json()).build();

    JobId jobId = JobId.newBuilder().setLocation(dataset.getLocation()).build();
    TableDataWriteChannel writer = bq.writer(jobId, writeChannelConfiguration);

    try (OutputStream stream = Channels.newOutputStream(writer)) {
      Files.copy(jsonPath, stream);
    }

    Job job = writer.getJob();
    job.waitFor();
  }

  private Field[] getSimpleFieldsSchema() {
    return new Field[]{
      Field.of("string_value", LegacySQLTypeName.STRING),
      Field.of("int_value", LegacySQLTypeName.INTEGER),
      Field.of("float_value", LegacySQLTypeName.FLOAT),
      Field.of("boolean_value", LegacySQLTypeName.BOOLEAN)
    };
  }

  private Field[] getUpdatedFieldsSchema() {
    return new Field[]{
      Field.of("string_value", LegacySQLTypeName.STRING),
      Field.of("int_value", LegacySQLTypeName.INTEGER),
      Field.of("float_value", LegacySQLTypeName.FLOAT),
      Field.of("boolean_value", LegacySQLTypeName.BOOLEAN),
      Field.of("numeric_value", LegacySQLTypeName.NUMERIC),
      Field.of("timestamp_value", LegacySQLTypeName.TIMESTAMP),
      Field.of("date_value", LegacySQLTypeName.DATE)
    };
  }

  private Field[] getFullFieldsSchema() {
    return new Field[] {
      Field.of("string_value", LegacySQLTypeName.STRING),
      Field.of("int_value", LegacySQLTypeName.INTEGER),
      Field.of("float_value", LegacySQLTypeName.FLOAT),
      Field.of("numeric_value", LegacySQLTypeName.NUMERIC),
      Field.of("boolean_value", LegacySQLTypeName.BOOLEAN),
      Field.of("timestamp_value", LegacySQLTypeName.TIMESTAMP),
      Field.of("date_value", LegacySQLTypeName.DATE),
      Field.of("time_value", LegacySQLTypeName.TIME),
      Field.of("datetime_value", LegacySQLTypeName.DATETIME),
      Field.newBuilder("string_array", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build()
    };
  }

  private Schema getSimpleTableSchem() {
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
                Schema.Field.of("string_array", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING))))
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
