# R2DBC Driver for YashanDB

基于 [R2DBC SPI](https://r2dbc.io/) 的崖山数据库（YashanDB）响应式驱动，底层通过 YashanDB JDBC 驱动桥接实现。

## 环境要求

| 组件 | 版本 |
|------|------|
| Java | 17+ |
| YashanDB JDBC | 1.9.24 |
| R2DBC SPI | 1.0.0.RELEASE |
| Reactor Core | 3.6.x |

## 安装

### 添加依赖

```xml
<dependency>
    <groupId>io.r2dbc</groupId>
    <artifactId>r2dbc-yashandb</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```xml
<!-- r2dbc-yashandb 本地 jar -->
<dependency>
    <groupId>com.yashandb</groupId>
    <artifactId>r2dbc-yashandb</artifactId>
    <version>0.1.2</version>
    <scope>system</scope>
    <systemPath>${project.basedir}/libs/r2dbc-yashandb-0.1.2.jar</systemPath>
</dependency>
```

## 快速开始

### 通过 URL 创建连接工厂

```java
ConnectionFactory factory = ConnectionFactories.get(
    "r2dbc:yashandb://sys:Enmo_123@192.168.163.134:1688/"
);
```

启用 SSL：

```java
ConnectionFactory factory = ConnectionFactories.get(
    "r2dbcs:yashandb://sys:Enmo_123@192.168.163.134:1688/"
);
```

### 通过编程方式创建连接工厂

```java
ConnectionFactory factory = new YashanDbConnectionFactory(
    YashanDbConnectionConfiguration.builder()
        .host("192.168.163.134")
        .port(1688)
        .database("")          // 留空即可，URL 格式为 jdbc:yasdb://host:port/
        .username("sys")
        .password("Enmo_123")
        .connectTimeout(Duration.ofSeconds(10))
        .ssl(false)
        .build()
);
```

### 执行查询

```java
Mono.from(factory.create())
    .flatMapMany(conn ->
        Flux.from(conn.createStatement("SELECT ID, NAME FROM MY_TABLE WHERE ID = :id")
                .bind("id", 1)
                .execute())
            .flatMap(result -> result.map((row, meta) -> {
                Integer id   = row.get("ID",   Integer.class);
                String  name = row.get("NAME", String.class);
                return id + " - " + name;
            }))
            .doFinally(s -> Mono.from(conn.close()).subscribe())
    )
    .subscribe(System.out::println);
```

### 事务

```java
Mono.from(factory.create())
    .flatMap(conn ->
        Mono.from(conn.beginTransaction())
            .then(Mono.from(conn.createStatement("INSERT INTO T VALUES (1, 'hello')").execute())
                .flatMap(r -> Mono.from(r.getRowsUpdated())))
            .then(Mono.from(conn.commitTransaction()))
            .onErrorResume(e -> Mono.from(conn.rollbackTransaction()).then(Mono.error(e)))
            .then(Mono.from(conn.close()))
    )
    .subscribe();
```

### Batch 执行

```java
Mono.from(factory.create())
    .flatMapMany(conn ->
        Flux.from(conn.createBatch()
                .add("INSERT INTO T VALUES (1)")
                .add("INSERT INTO T VALUES (2)")
                .add("INSERT INTO T VALUES (3)")
                .execute())
            .flatMap(r -> Mono.from(r.getRowsUpdated()))
            .doFinally(s -> Mono.from(conn.close()).subscribe())
    )
    .subscribe();
```

## 连接配置参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `host` | String | `localhost` | 数据库主机名或 IP |
| `port` | int | `1688` | 数据库端口 |
| `database` | String | `""` | 数据库名，通常留空 |
| `username` | String | — | 登录用户名（必填） |
| `password` | CharSequence | — | 登录密码（必填） |
| `connectTimeout` | Duration | `10s` | TCP 连接超时 |
| `ssl` | boolean | `false` | 是否启用 SSL/TLS |

## 支持的数据类型

| Java 类型 | YashanDB 类型 |
|-----------|--------------|
| `String` | VARCHAR2, CHAR, CLOB, NVARCHAR2 等 |
| `Integer` | NUMBER, INTEGER |
| `Long` | NUMBER, BIGINT |
| `Double` | DOUBLE, FLOAT |
| `Float` | FLOAT |
| `BigDecimal` | NUMBER |
| `Boolean` | NUMBER(1) |
| `LocalDate` | DATE |
| `LocalDateTime` | TIMESTAMP |
| `LocalTime` | TIME |
| `OffsetDateTime` | TIMESTAMP WITH TIME ZONE |
| `byte[]` | RAW, BLOB |
| `Blob` | BLOB |
| `Clob` | CLOB |
| `Object` | 任意类型（回退编解码） |

## 构建

```bash
mvn clean package
```

## 测试

### 配置测试数据库连接

集成测试和 TCK 测试需要连接真实的 YashanDB 实例。连接参数从测试类路径下的 `src/test/resources/test-database.properties` 文件读取(若文件不存在，可手动创建)：

```properties
db.host=192.168.163.134
db.port=1688
db.database=
db.user=sys
db.password=your_password
```

也可以在运行时通过 `-D` 系统属性覆盖文件中的值，无需修改文件：

```bash
mvn test -Ddb.host=<host> -Ddb.port=<port> -Ddb.user=<user> -Ddb.password=<password>
```

### 运行单元测试（无需数据库）

```bash
mvn test -Dtest="YashanDbStatementTest,YashanDbTypeTest,CodecTest,YashanDbConnectionFactoryTest"
```

### 运行集成测试

```bash
mvn test -Dtest=IntegrationTest
```

### 运行 R2DBC SPI TCK（兼容性测试套件）

TCK（Technology Compatibility Kit）基于 [r2dbc-spi-test](https://github.com/r2dbc/r2dbc-spi) 验证驱动对 R2DBC SPI 规范的完整实现。

```bash
mvn test -Dtest=YashanDbTestKit
```

运行结果示例：

```
Tests run: 32, Failures: 0, Errors: 0, Skipped: 2
```

> 其中 2 个用例因 YashanDB 不支持的特性被 `@Disabled`：
> - `compoundStatement`：YashanDB 不支持在单条 `createStatement()` 中执行 `;` 分隔的多条语句。开启`allowMultiStmt=true`后也只能返回第一条语句的结果。
> - `returnGeneratedValues`：YashanDB JDBC 驱动对非 IDENTITY 列不支持 `RETURN_GENERATED_KEYS`

### 运行全部测试

```bash
mvn test
```

## 项目结构

```
src/main/java/io/r2dbc/yashandb/
├── codec/                          # 类型编解码器
│   ├── Codec.java
│   ├── DefaultCodecs.java
│   └── *Codec.java                 # 各类型编解码实现
├── YashanDbBatch.java              # Batch 语句执行
├── YashanDbConnection.java         # Connection 实现
├── YashanDbConnectionConfiguration.java  # 连接配置
├── YashanDbConnectionFactory.java  # ConnectionFactory 实现
├── YashanDbConnectionFactoryMetadata.java
├── YashanDbConnectionFactoryProvider.java  # SPI 注册入口
├── YashanDbConnectionMetadata.java
├── YashanDbResult.java             # Result 实现
├── YashanDbRow.java                # Row 实现
├── YashanDbRowMetadata.java
├── YashanDbStatement.java          # Statement 实现
└── YashanDbType.java               # 类型映射
```

## R2DBC VS JDBC 性能对比测试
<https://github.com/Dark-Athena/yashandb-r2dbc-vs-jdbc-test>
## License

Apache License 2.0
