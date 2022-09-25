# <a name="dp-203t00-data-engineering-on-azure"></a>DP-203T00：Azure 上的数据工程

Welcome to the course DP-203: Data Engineering on Azure. To support this course, we will need to make updates to the course content to keep it current with the Azure services used in the course.  We are publishing the lab instructions and lab files on GitHub to allow for open contributions between the course authors and MCTs to keep the content current with changes in the Azure platform.

## <a name="lab-overview"></a>实验室概述

以下为每个模块的实验室目标摘要：

### <a name="day-1"></a>第 1 天

#### <a name="module-00-lab-environment-setup"></a>[模块 00：实验室环境设置](Instructions/Labs/LAB_00_lab_setup_instructions.md)

完成本课程的实验室环境设置。

#### <a name="module-01-explore-compute-and-storage-options-for-data-engineering-workloads"></a>[模块 01：探索用于数据工程工作负载的计算和存储选项](Instructions/Labs/LAB_01_compute_and_storage_options.md)

This lab teaches ways to structure the data lake, and to optimize the files for exploration, streaming, and batch workloads. The student will learn how to organize the data lake into levels of data refinement as they transform files through batch and stream processing. The students will also experience working with Apache Spark in Azure Synapse Analytics.  They will learn how to create indexes on their datasets, such as CSV, JSON, and Parquet files, and use them for potential query and workload acceleration using Spark libraries including Hyperspace and MSSParkUtils.

#### <a name="module-02-run-interactive-queries-using-azure-synapse-analytics-serverless-sql-pools"></a>[模块 02：使用 Azure Synapse Analytics 无服务器 SQL 池运行交互式查询](Instructions/Labs/LAB_02_queries_using_serverless_sql_pools.md)

欢迎学习课程 DP-203：Azure 上的数据工程。

#### <a name="module-03-data-exploration-and-transformation-in-azure-databricks"></a>[模块 03：Azure Databricks 中的数据探索和转换](Instructions/Labs/LAB_03_data_transformation_in_databricks.md)

为了支持这门课程，我们需要对课程内容进行更新，使其与本课程中使用的 Azure 服务保持一致。

### <a name="day-2"></a>第 2 天

#### <a name="module-04-explore-transform-and-load-data-into-the-data-warehouse-using-apache-spark"></a>[模块 04：使用 Apache Spark 探索、转换数据并将数据加载到数据仓库](Instructions/Labs/LAB_04_data_warehouse_using_apache_spark.md)

我们将在 GitHub 上发布实验室说明和实验室文件，以允许课程作者和 MCT 之间的开放式协作，从而让内容与 Azure 平台中的更改保持同步。

#### <a name="module-05-ingest-and-load-data-into-the-data-warehouse"></a>[模块 05：将数据引入和加载到数据仓库中](Instructions/Labs/LAB_05_load_data_into_the_data_warehouse.md)

This lab teaches students how to ingest data into the data warehouse through T-SQL scripts and Synapse Analytics integration pipelines. The student will learn how to load data into Synapse dedicated SQL pools with PolyBase and COPY using T-SQL. The student will also learn how to use workload management along with a Copy activity in a Azure Synapse pipeline for petabyte-scale data ingestion.

#### <a name="module-06-transform-data-with-azure-data-factory-or-azure-synapse-pipelines"></a>[模块 06：使用 Azure 数据工厂或 Azure Synapse 管道转换数据](Instructions/Labs/LAB_06_transform_data_with_pipelines.md)

本实验室教学生如何执行以下操作：生成数据集成管道以从多个数据源引入、使用映射数据流和笔记本转换数据、将数据移动到一个或多个数据接收器中。

### <a name="day-3"></a>第 3 天

#### <a name="module-07-integrate-data-from-notebooks-with-azure-data-factory-or-azure-synapse-pipelines"></a>[模块 07：将笔记本中的数据与 Azure 数据工厂或 Azure Synapse 管道集成](Instructions/Labs/LAB_07_integrate_data_from_notebooks.md)

In the lab, the students will create a notebook to query user activity and purchases that they have made in the past 12 months. They will then add the notebook to a pipeline using the new Notebook activity and execute this notebook after the Mapping Data Flow as part of their orchestration process. While configuring this the students will implement parameters to add dynamic content in the control flow and validate how the parameters can be used.

#### <a name="module-08-end-to-end-security-with-azure-synapse-analytics"></a>[模块 08：使用 Azure Synapse Analytics 实现端到端安全性](Instructions/Labs/LAB_08_security_with_synapse_analytics.md)

In this lab, students will learn how to secure a Synapse Analytics workspace and its supporting infrastructure. The student will observe the SQL Active Directory Admin, manage IP firewall rules, manage secrets with Azure Key Vault and access those secrets through a Key Vault linked service and pipeline activities. The student will understand how to implement column-level security, row-level security, and dynamic data masking when using dedicated SQL pools.

#### <a name="module-09-support-hybrid-transactional-analytical-processing-htap-with-azure-synapse-link"></a>[模块 09：使用 Azure Synapse Link 支持混合事务分析处理 (HTAP)](Instructions/Labs/LAB_09_htap_with_azure_synapse_link.md)

This lab teaches you how Azure Synapse Link enables seamless connectivity of an Azure Cosmos DB account to a Synapse workspace. You will understand how to enable and configure Synapse link, then how to query the Azure Cosmos DB analytical store using Apache Spark and SQL Serverless.
### <a name="day-4"></a>第 4 天
#### <a name="module-10-real-time-stream-processing-with-stream-analytics"></a>[模块 10：使用流分析进行实时流处理](Instructions/Labs/LAB_10_stream_analytics.md)

This lab teaches you how to process streaming data with Azure Stream Analytics. You will ingest vehicle telemetry data into Event Hubs, then process that data in real time, using various windowing functions in Azure Stream Analytics. You will output the data to Azure Synapse Analytics. Finally, you will learn how to scale the Stream Analytics job to increase throughput.

#### <a name="module-11-create-a-stream-processing-solution-with-event-hubs-and-azure-databricks"></a>[模块 11：使用事件中心和 Azure Databricks 创建流处理解决方案](Instructions/Labs/LAB_11_stream_with_azure_databricks.md)

This lab teaches you how to ingest and process streaming data at scale with Event Hubs and Spark Structured Streaming in Azure Databricks. You will learn the key features and uses of Structured Streaming. You will implement sliding windows to aggregate over chunks of data and apply watermarking to remove stale data. Finally, you will connect to Event Hubs to read and write streams.

- <bpt id="p1">**</bpt>Are you a MCT?<ept id="p1">**</ept> - Have a look at our <bpt id="p1">[</bpt>GitHub User Guide for MCTs<ept id="p1">](https://microsoftlearning.github.io/MCT-User-Guide/)</ept>.
                                                                       
## <a name="how-should-i-use-these-files-relative-to-the-released-moc-files"></a>我应该如何使用与已发布 MOC 文件相关的这些文件？

- 讲师手册和 PowerPoint 仍将是教授课程内容的主要来源。

- GitHub 上的这些文件适合与学生手册结合使用，不过在 GitHub 中作为中央存储库使用，因此 MCT 和课程作者可以获得一个用于共享最新实验室文件的共享源。

- 本实验室介绍用于构造 Data Lake 以及优化用于探索、流式处理和批处理工作负载的文件的方法。

- 对于每次授课，建议培训师查看 GitHub 中的内容，了解是否为支持最新 Azure 服务而进行了更改，并获取最新文件来进行授课。

- 学生将学习如何将数据湖组织为各级数据细化，因为他们将通过批处理和流处理转换文件。

## <a name="what-about-changes-to-the-student-handbook"></a>如何处理对学生手册的更改？

- 我们将按季度审核学员手册，并根据需要通过标准 MOC 版本通道进行更新。

## <a name="how-do-i-contribute"></a>如何参与内容编辑？

- 任何 MCT 都可对 GitHub 存储库中的代码或内容提出问题，Microsoft 和课程作者将根据需要进行分类，包括内容和实验室代码更改。

## <a name="classroom-materials"></a>课堂材料

学生还将体验在 Azure Synapse Analytics 中使用 Apache Spark。

## <a name="what-are-we-doing"></a>我们要做什么？

- 他们将了解如何对其数据集（例如 CSV、JSON 和 Parquet 文件）创建索引，并使用包括 Hyperspace 和 MSSParkUtils 在内的 Spark 库将这些索引用于潜在的查询和工作负载加速。

- We hope that this brings a sense of collaboration to the labs like we've never had before - when Azure changes and you find it first during a live delivery, go ahead and make an enhancement right in the lab source.  Help your fellow MCTs.

## <a name="how-should-i-use-these-files-relative-to-the-released-moc-files"></a>我应该如何使用与已发布 MOC 文件相关的这些文件？

- 讲师手册和 PowerPoint 仍将是教授课程内容的主要来源。

- GitHub 上的这些文件旨在与学员手册一起使用，但在 GitHub 中是作为中心存储库，因此 MCT 和课程作者可以拥有最新实验室文件的共享源。

- 建议在每次交付时，培训师检查 GitHub 是否为支持最新的 Azure 服务进行了任何更改，并获取最新的交付文件。

## <a name="what-about-changes-to-the-student-handbook"></a>学员手册有什么变化？

- 我们将按季度审核学员手册，并根据需要通过标准 MOC 版本通道进行更新。

## <a name="how-do-i-contribute"></a>如何做出贡献？

- 任何 MCT 都可向 GitHub 存储库中的代码或内容提交拉取请求，Microsoft 和课程作者将根据需要进行分流并纳入内容和实验室代码的更改。

- You can submit bugs, changes, improvement and ideas.  Find a new Azure feature before we have?  Submit a new demo!

## <a name="notes"></a>说明

### <a name="classroom-materials"></a>课堂材料

在本实验室中，学生将了解如何通过 Azure Synapse Analytics 中的无服务器 SQL 池执行的 T-SQL 语句使用 Data Lake 中的文件和外部文件源。
