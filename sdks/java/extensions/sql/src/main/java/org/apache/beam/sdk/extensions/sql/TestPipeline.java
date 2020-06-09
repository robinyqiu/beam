/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.format.DateTimeFormat;

// Add dependency to Dataflow Runner
// shadow project(path: ":beam-runners-google-cloud-dataflow-java", configuration: "shadow")
//
// To run as a gradle task
// task execute(type: JavaExec) {
//     main = "org.apache.beam.sdk.extensions.sql.TestPipeline"
//     classpath = sourceSets.main.runtimeClasspath
// }
public class TestPipeline {

  public static PCollection<Row> ReadFromQuery(
      Pipeline p, String table_header[], Schema tableSchema, String tablename, String query) {
    PCollection<Row> LookupTable = p.apply("Read From " + tablename,
        BigQueryIO.readTableRows().fromQuery(query).usingStandardSql())
        .apply("ConvertToRow", ParDo.of(new DoFn<TableRow,Row>() {
                                          private static final long serialVersionUID = 1L;

                                          @ProcessElement
                                          public void processElement(ProcessContext c) throws IOException {

                                            String values[]=new String[table_header.length];
                                            for(int i=0;i<table_header.length;i++)
                                            {
                                              if(c.element().get(table_header[i])==null)
                                              {
                                                values[i]="";
                                              }
                                              else
                                              {
                                                values[i]=c.element().get(table_header[i]).toString();
                                              }
                                            }
                                            Row row = Row.withSchema(tableSchema).addValues((Object[]) values).build();

                                            c.output(row);

                                          }
                                        }

        ));
    LookupTable.setRowSchema(tableSchema);

    return LookupTable;
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DataflowRunner.class);
    options.as(DataflowPipelineOptions.class).setProject("google.com:clouddfe");
    options.as(DataflowPipelineOptions.class).setTempLocation("gs://robinyq/tmp");
    Pipeline p = Pipeline.create(options);

    String header="LEARNINGPROVIDERID,NAME,UNITREF,UNITNAME,UNITLEVEL,EXAMDATE,DATECREATED,TESTTYPE,TOTALAMOUNT,DESCRIPTION,LEARNERID,LEARNERREF,GRADEID,GRADEDESCRIPTION,FIRSTNAME,MIDDLENAME,LASTNAME,DATEOFBIRTH,GENDER,LEGALSEXTYPE,REGREF,REGISTRATIONPRODUCTID,QUALIFICATIONID,REGISTPRODUCT,MIGRATIONCODE,PRODUCTNAME,ENTRYID,TESTTYPE_DF";
    String header1[]=header.split(",");

    //Add all header columns here
    Schema fileSchema = Schema.builder().addStringField("LEARNINGPROVIDERID").addStringField("NAME").addStringField("UNITREF").addStringField("UNITNAME").addStringField("UNITLEVEL").addStringField("EXAMDATE").addStringField("DATECREATED").addStringField("TESTTYPE").addStringField("TOTALAMOUNT").addStringField("DESCRIPTION").addStringField("LEARNERID").addStringField("LEARNERREF").addStringField("GRADEID").addStringField("GRADEDESCRIPTION").addStringField("FIRSTNAME").addStringField("MIDDLENAME").addStringField("LASTNAME").addStringField("DATEOFBIRTH").addStringField("GENDER").addStringField("LEGALSEXTYPE").addStringField("REGREF").addStringField("REGISTRATIONPRODUCTID").addStringField("QUALIFICATIONID").addStringField("REGISTPRODUCT").addStringField("MIGRATIONCODE").addStringField("PRODUCTNAME").addStringField("ENTRYID").addStringField("TESTTYPE_DF").build();

    //Read from storage and convert to Row
    PCollection<Row> InputFiles=p.apply("Read From Storage",
        TextIO.read().from("gs://robinyq/qma.csv"))
        .apply("ConvertToRow",ParDo.of(new DoFn<String,Row>() {
                                         private static final long serialVersionUID = 1L;

                                         @ProcessElement
                                         public void processElement(ProcessContext c) throws IOException {

                                           if (!c.element().startsWith(header1[0]))
                                           {
                                             String line = c.element();
                                             CSVParser csvParser= CSVParser.parse(line, CSVFormat.DEFAULT);
                                             String values[]=new String[header1.length];
                                             for(CSVRecord record: csvParser)
                                             {
                                               for(int i=0;i<header1.length;i++)
                                               {
                                                 if(record.get(i).equals("NULL"))
                                                 {
                                                   values[i]="";
                                                 }
                                                 else {
                                                   values[i]=record.get(i).trim();
                                                 }
                                               }

                                             }
                                             Row row = Row.withSchema(fileSchema).addValues((Object[]) values).build();

                                             c.output(row);

                                           }
                                         }

                                       }
        ));
    InputFiles.setRowSchema(fileSchema);

    String table_header1[]= {"sourceFeedName_name","name","externalSysReference_identifier","productSourcePrimaryKey","productPrimaryKey","externalSysReference_identityProvider_name","isLeadRecord"};
    Schema tableSchema1 = Schema.builder().addStringField("sourceFeedName_name").addStringField("name").addStringField("externalSysReference_identifier").addStringField("productSourcePrimaryKey").addStringField("productPrimaryKey").addStringField("externalSysReference_identityProvider_name").addStringField("isLeadRecord")
        .build();
    String q1="SELECT sourceFeedName.name AS sourceFeedName_name,name,externalSysReference.identifier AS externalSysReference_identifier,productSourcePrimaryKey,productPrimaryKey,externalSysReference.identityProvider.name AS externalSysReference_identityProvider_name,isLeadRecord FROM PearsonDevGCPSprt.UkProduct";
    PCollection<Row> UkProduct=ReadFromQuery(p,table_header1,tableSchema1,"PearsonDevGCPSprt.UkProduct",q1);

    String table_header2[]= {"externalSystemReference_identifier","onedataCustomerIdentifier","onedataCustomerNumber"};
    Schema tableSchema2 = Schema.builder().addStringField("externalSystemReference_identifier").addStringField("onedataCustomerIdentifier").addStringField("onedataCustomerNumber").build();
    String q2="SELECT externalSystemReference.identifier as externalSystemReference_identifier,onedataCustomerIdentifier,onedataCustomerNumber from PearsonDevGCPSprt.CustomerIdentifier where current_date() between activestartdate and activeenddate and  externalSystemReference.identityProvider ='QMACustomer.LearningProviderID'";
    PCollection<Row> CustomerIdentifier=ReadFromQuery(p,table_header2,tableSchema2,"PearsonDevGCPSprt.CustomerIdentifier",q2);

    String table_header3[]= {"qmaLearnerReference","learnerKey","learnerSourceKey"};
    Schema tableSchema3 = Schema.builder().addStringField("qmaLearnerReference").addStringField("learnerKey").addStringField("learnerSourceKey").build();
    String q3="SELECT * FROM (SELECT qmaLearnerReference,learnerKey,learnerSourceKey,row_number() over(partition by qmaLearnerReference order by learnerSourceKey desc) as rownum from PearsonDevGCPSprt.Learner where qmaLearnerReference is not null  order by qmaLearnerReference ) WHERE rownum=1";
    PCollection<Row> Learner=ReadFromQuery(p,table_header3,tableSchema3,"PearsonDevGCPSprt.Learner",q3);

    String table_header4[]= {"id","name"};
    Schema tableSchema4 = Schema.builder().addStringField("id").addStringField("name").build();
    String q4="SELECT id,name from PearsonDevGCPSprt.AssessmentOutcome";
    PCollection<Row> AssessmentOutcome=ReadFromQuery(p,table_header4,tableSchema4,"PearsonDevGCPSprt.AssessmentOutcome",q4);

    String table_header5[]= {"id","name"};
    Schema tableSchema5 = Schema.builder().addStringField("id").addStringField("name").build();
    String q5="SELECT id,name from PearsonDevGCPSprt.AssessmentType";
    PCollection<Row> AssessmentType=ReadFromQuery(p,table_header5,tableSchema5,"PearsonDevGCPSprt.AssessmentType",q5);

    PCollectionTuple JoinQuery = PCollectionTuple.of(new TupleTag<>("SRC_FILE"), InputFiles)
        .and(new TupleTag<>("UkProduct"), UkProduct)
        .and(new TupleTag<>("CustomerIdentifier"), CustomerIdentifier)
        .and(new TupleTag<>("Learner"), Learner)
        .and(new TupleTag<>("AssessmentOutcome"), AssessmentOutcome)
        .and(new TupleTag<>("AssessmentType"), AssessmentType);

    String query="SELECT 'QMA' AS `originatingSystem.id`,'QMA' AS `originatingSystem.name`,SOURCE_FILE.GRADEDESCRIPTION AS `assessmentOutcome.id`,SOURCE_FILE.EXAMDATE AS datekey,SOURCE_FILE.TESTTYPE_DF AS `assessmentType.id`,CASE WHEN (SOURCE_FILE.QUALIFICATIONID=NULL OR SOURCE_FILE.QUALIFICATIONID='') THEN '1' ELSE '0' END AS isSchemeRegistered,SOURCE_FILE.LEARNERREF AS learnerRegNumber,  \r\n" +
        "				CS.onedataCustomerIdentifier AS `assessmentCustomer.customerIdentifierKey`,CS.onedataCustomerNumber AS `assessmentCustomer.customerKey`, \r\n" +
        "				L.learnerKey AS `assessmentLearner.learnerKey`,L.learnerSourceKey AS `assessmentLearner.learnerSourceKey`,\r\n" +
        "				UKP1.productPrimaryKey AS `assessmentProduct.productPrimaryKey`,UKP1.productSourcePrimaryKey AS `assessmentProduct.productSourcePrimaryKey`,\r\n" +
        "				UKP2.productPrimaryKey AS `assessmentParentProduct.productPrimaryKey`,UKP2.productSourcePrimaryKey AS `assessmentParentProduct.productSourcePrimaryKey`,\r\n" +
        "				AO.name AS `assessmentOutcome.name`,ATY.name AS `assessmentType.name`, \r\n" +
        "				CASE WHEN (UKP1.name like '%ICT%' or UKP1.name like '%Information%') and (UKP1.name like '%L1%' or UKP1.name like '%Level 1%') THEN 'ICT Level 1' WHEN (UKP1.name like '%ICT%' or UKP1.name like '%Information%') and (UKP1.name like '%L2%' or UKP1.name like '%Level 2%') THEN 'ICT Level 2' WHEN (UKP1.name like '%Math%') and (UKP1.name like '%L1%' or UKP1.name like '%Level 1%') THEN 'Maths Level 1' WHEN (UKP1.name like '%Math%') and (UKP1.name like '%L2%' or UKP1.name like '%Level 2%') THEN 'Maths Level 2' WHEN (UKP1.name like '%Read%') and (UKP1.name like '%L1%' or UKP1.name like '%Level 1%') THEN 'Reading Level 1' WHEN (UKP1.name like '%Read%') and (UKP1.name like '%L2%' or UKP1.name like '%Level 2%') THEN 'Reading Level 2' WHEN (UKP1.name like '%Writ%') and (UKP1.name like '%L1%' or UKP1.name like '%evel 1%') THEN 'Writing Level 1' WHEN (UKP1.name like '%Writ%') and (UKP1.name like '%L2%' or UKP1.name like '%Level 2%') THEN 'Writing Level 2' END AS unitNameCleaned   \r\n" +
        "				FROM SRC_FILE SOURCE_FILE \r\n" +
        "				LEFT OUTER JOIN Learner L ON SOURCE_FILE.LEARNERREF=L.qmaLearnerReference \r\n" +
        "				LEFT OUTER JOIN CustomerIdentifier CS ON SOURCE_FILE.LEARNINGPROVIDERID=CS.externalSystemReference_identifier \r\n" +
        "				LEFT OUTER JOIN UkProduct UKP1 ON SOURCE_FILE.UNITREF=UKP1.externalSysReference_identifier AND UKP1.externalSysReference_identityProvider_name in ('QMA') AND UKP1.sourceFeedName_name in ('QMA Units') \r\n" +
        "				LEFT OUTER JOIN UkProduct UKP2 ON SOURCE_FILE.REGISTPRODUCT = UKP2.externalSysReference_identifier " +
        "				LEFT OUTER JOIN AssessmentOutcome AO ON SOURCE_FILE.GRADEDESCRIPTION=AO.id " +
        "				LEFT OUTER JOIN AssessmentType ATY ON SOURCE_FILE.TESTTYPE_DF=ATY.id";

    PCollection<Row> QueryOutput =
        JoinQuery.apply("Execute Query", SqlTransform.query(query).registerUdf("eval", DateBetween.class));

    p.run().waitUntilFinish();
  }

  public static class DateBetween implements BeamSqlUdf {
    public static Boolean eval(String input1){

      String str_searchDate = input1.toString().trim().split("#")[0].split(" ")[0];
      String str_activeStartDate = input1.toString().trim().split("#")[1];
      String str_activeEndDate = input1.toString().trim().split("#")[2];

      if (str_searchDate.toString().trim().length()< 10 || str_activeStartDate.toString().trim().length()< 10 || str_activeEndDate.toString().trim().length()< 10)
      {
        return false;
      }
      else
      {
        org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        org.joda.time.DateTime ST_REG_DATE = formatter.parseDateTime(str_searchDate);
        org.joda.time.DateTime activeStartDate = formatter.parseDateTime(str_activeStartDate);
        org.joda.time.DateTime activeEndDate = formatter.parseDateTime(str_activeEndDate);

        if((ST_REG_DATE.equals(activeStartDate) || ST_REG_DATE.isAfter(activeStartDate)) && (ST_REG_DATE.equals(activeEndDate) || ST_REG_DATE.isBefore(activeEndDate)))
          return true;
        else
          return false;
      }
    }
  }
}