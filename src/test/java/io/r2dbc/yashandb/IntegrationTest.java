package io.r2dbc.yashandb;

import io.r2dbc.spi.*;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Integration tests against a real YashanDB instance.
 *
 * <p>Connection parameters are loaded from {@code test-database.properties} on the test
 * classpath, and can be overridden at runtime via system properties:
 * <pre>{@code
 *   mvn test -Dtest=IntegrationTest -Ddb.host=... -Ddb.port=... -Ddb.user=... -Ddb.password=...
 * }</pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IntegrationTest {

    private static ConnectionFactory factory;

    @BeforeAll
    static void setup() {
        factory = new YashanDbConnectionFactory(
                YashanDbConnectionConfiguration.builder()
                        .host(TestDatabaseConfig.HOST)
                        .port(TestDatabaseConfig.PORT)
                        .database(TestDatabaseConfig.DATABASE)
                        .username(TestDatabaseConfig.USER)
                        .password(TestDatabaseConfig.PASSWORD)
                        .build()
        );
    }

    // ------------------------------------------------------------------ helpers

    private Mono<Connection> connect() {
        return Mono.from(factory.create());
    }

    private <T> T block(Mono<T> mono) {
        return mono.block(java.time.Duration.ofSeconds(30));
    }

    // ------------------------------------------------------------------ tests

    @Test
    @Order(1)
    @DisplayName("1. 连接工厂元数据")
    void factoryMetadata() {
        ConnectionFactoryMetadata meta = factory.getMetadata();
        System.out.println("[META] name = " + meta.getName());
        Assertions.assertNotNull(meta.getName());
    }

    @Test
    @Order(2)
    @DisplayName("2. 建立连接并验证")
    void createAndValidate() {
        Mono<Boolean> test = connect()
                .flatMap(conn -> Mono.from(conn.validate(ValidationDepth.REMOTE))
                        .doFinally(s -> Mono.from(conn.close()).subscribe()));
        StepVerifier.create(test)
                .expectNext(Boolean.TRUE)
                .verifyComplete();
    }

    @Test
    @Order(3)
    @DisplayName("3. 连接元数据（数据库版本）")
    void connectionMetadata() {
        block(connect().flatMap(conn -> {
            ConnectionMetadata meta = conn.getMetadata();
            System.out.println("[CONN META] databaseProductName  = " + meta.getDatabaseProductName());
            System.out.println("[CONN META] databaseVersion      = " + meta.getDatabaseVersion());
            Assertions.assertNotNull(meta.getDatabaseProductName());
            return Mono.from(conn.close());
        }));
    }

    @Test
    @Order(4)
    @DisplayName("4. 简单 SELECT 1")
    void selectOne() {
        List<Integer> rows = block(
                connect().flatMapMany(conn ->
                        Flux.from(conn.createStatement("SELECT 1 AS VAL FROM DUAL").execute())
                                .flatMap(result -> result.map((row, meta) -> row.get("VAL", Integer.class)))
                                .doFinally(s -> Mono.from(conn.close()).subscribe())
                )
                .collectList()
        );
        System.out.println("[SELECT 1] result = " + rows);
        Assertions.assertEquals(List.of(1), rows);
    }

    @Test
    @Order(5)
    @DisplayName("5. 查询数据库版本字符串")
    void selectVersion() {
        List<String> rows = block(
                connect().flatMapMany(conn ->
                        Flux.from(conn.createStatement("SELECT BANNER FROM V$VERSION").execute())
                                .flatMap(result -> result.map((row, meta) -> row.get(0, String.class)))
                                .doFinally(s -> Mono.from(conn.close()).subscribe())
                )
                .collectList()
        );
        rows.forEach(v -> System.out.println("[VERSION] " + v));
        Assertions.assertFalse(rows.isEmpty());
    }

    @Test
    @Order(6)
    @DisplayName("6. DDL：建表 / 删表")
    void ddl() {
        block(connect().flatMap(conn ->
                Mono.from(conn.createStatement(
                                "CREATE TABLE R2DBC_TEST_TBL (" +
                                "  ID       NUMBER(10) PRIMARY KEY," +
                                "  NAME     VARCHAR2(100)," +
                                "  SCORE    NUMBER(10,2)," +
                                "  CREATED  DATE" +
                                ")")
                        .execute())
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .doOnNext(n -> System.out.println("[DDL CREATE] rowsUpdated=" + n))
                        .then(Mono.from(conn.createStatement("DROP TABLE R2DBC_TEST_TBL").execute()))
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .doOnNext(n -> System.out.println("[DDL DROP] rowsUpdated=" + n))
                        .then(Mono.from(conn.close()))
        ));
    }

    @Test
    @Order(7)
    @DisplayName("7. DML：INSERT / SELECT / UPDATE / DELETE（参数绑定）")
    void dml() {
        block(connect().flatMap(conn ->
                // 建表
                Mono.from(conn.createStatement(
                                "CREATE TABLE R2DBC_DML_TBL (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(100), SCORE NUMBER(10,2), CREATED DATE)")
                        .execute())
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        // INSERT 3 行
                        .then(Flux.range(1, 3)
                                .concatMap(i -> Mono.from(
                                        conn.createStatement("INSERT INTO R2DBC_DML_TBL VALUES (:id, :name, :score, :created)")
                                                .bind("id",      i)
                                                .bind("name",    "user_" + i)
                                                .bind("score",   BigDecimal.valueOf(i * 10.5))
                                                .bind("created", LocalDate.of(2024, 1, i))
                                                .execute())
                                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                                        .doOnNext(n -> System.out.println("[INSERT] id=" + i + " rowsUpdated=" + n))
                                )
                                .then()
                        )
                        // SELECT
                        .thenMany(
                                Flux.from(conn.createStatement("SELECT ID, NAME, SCORE, CREATED FROM R2DBC_DML_TBL ORDER BY ID").execute())
                                        .flatMap(result -> result.map((row, meta) -> {
                                            Integer id      = row.get("ID",      Integer.class);
                                            String  name    = row.get("NAME",    String.class);
                                            BigDecimal score = row.get("SCORE", BigDecimal.class);
                                            LocalDate created = row.get("CREATED", LocalDate.class);
                                            System.out.println("[SELECT] id=" + id + " name=" + name + " score=" + score + " created=" + created);
                                            return id;
                                        }))
                        )
                        .collectList()
                        .doOnNext(ids -> Assertions.assertEquals(List.of(1, 2, 3), ids))
                        // UPDATE
                        .then(
                                Mono.from(conn.createStatement("UPDATE R2DBC_DML_TBL SET NAME = :name WHERE ID = :id")
                                        .bind("name", "updated")
                                        .bind("id",   1)
                                        .execute())
                                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                                        .doOnNext(n -> {
                                            System.out.println("[UPDATE] rowsUpdated=" + n);
                                            Assertions.assertEquals(1L, (long) n);
                                        })
                        )
                        // DELETE
                        .then(
                                Mono.from(conn.createStatement("DELETE FROM R2DBC_DML_TBL WHERE ID > :id")
                                        .bind("id", 1)
                                        .execute())
                                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                                        .doOnNext(n -> {
                                            System.out.println("[DELETE] rowsUpdated=" + n);
                                            Assertions.assertEquals(2L, (long) n);
                                        })
                        )
                        // 清理
                        .then(Mono.from(conn.createStatement("DROP TABLE R2DBC_DML_TBL").execute()))
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.close()))
        ));
    }

    @Test
    @Order(8)
    @DisplayName("8. 事务提交")
    void transactionCommit() {
        block(connect().flatMap(conn ->
                Mono.from(conn.createStatement(
                                "CREATE TABLE R2DBC_TX_TBL (ID NUMBER(10) PRIMARY KEY, VAL VARCHAR2(50))")
                        .execute())
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.beginTransaction()))
                        .then(Mono.from(conn.createStatement("INSERT INTO R2DBC_TX_TBL VALUES (1, 'hello')")
                                .execute())
                                .flatMap(r -> Mono.from(r.getRowsUpdated())))
                        .doOnNext(n -> System.out.println("[TX INSERT] rowsUpdated=" + n))
                        .then(Mono.from(conn.commitTransaction()))
                        .doOnSuccess(v -> System.out.println("[TX] committed"))
                        // 验证数据持久化
                        .thenMany(Flux.from(conn.createStatement("SELECT VAL FROM R2DBC_TX_TBL WHERE ID = 1").execute())
                                .flatMap(r -> r.map((row, m) -> row.get("VAL", String.class))))
                        .collectList()
                        .doOnNext(vals -> {
                            System.out.println("[TX SELECT] " + vals);
                            Assertions.assertEquals(List.of("hello"), vals);
                        })
                        // 清理
                        .then(Mono.from(conn.createStatement("DROP TABLE R2DBC_TX_TBL").execute()))
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.close()))
        ));
    }

    @Test
    @Order(9)
    @DisplayName("9. 事务回滚")
    void transactionRollback() {
        block(connect().flatMap(conn ->
                Mono.from(conn.createStatement(
                                "CREATE TABLE R2DBC_RB_TBL (ID NUMBER(10) PRIMARY KEY)")
                        .execute())
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.beginTransaction()))
                        .then(Mono.from(conn.createStatement("INSERT INTO R2DBC_RB_TBL VALUES (99)").execute())
                                .flatMap(r -> Mono.from(r.getRowsUpdated())))
                        .then(Mono.from(conn.rollbackTransaction()))
                        .doOnSuccess(v -> System.out.println("[ROLLBACK] done"))
                        // 验证数据不存在
                        .thenMany(Flux.from(conn.createStatement("SELECT COUNT(*) AS CNT FROM R2DBC_RB_TBL").execute())
                                .flatMap(r -> r.map((row, m) -> row.get("CNT", Integer.class))))
                        .collectList()
                        .doOnNext(vals -> {
                            System.out.println("[ROLLBACK COUNT] " + vals);
                            Assertions.assertEquals(List.of(0), vals);
                        })
                        .then(Mono.from(conn.createStatement("DROP TABLE R2DBC_RB_TBL").execute()))
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.close()))
        ));
    }

    @Test
    @Order(10)
    @DisplayName("10. NULL 值读写")
    void nullValues() {
        block(connect().flatMap(conn ->
                Mono.from(conn.createStatement(
                                "CREATE TABLE R2DBC_NULL_TBL (ID NUMBER(10), NAME VARCHAR2(50))")
                        .execute())
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.createStatement("INSERT INTO R2DBC_NULL_TBL VALUES (1, NULL)").execute())
                                .flatMap(r -> Mono.from(r.getRowsUpdated())))
                        .thenMany(Flux.from(conn.createStatement("SELECT NAME FROM R2DBC_NULL_TBL WHERE ID = 1").execute())
                                .flatMap(r -> r.map((row, m) -> {
                                    String val = row.get("NAME", String.class);
                                    System.out.println("[NULL] NAME = " + val);
                                    Assertions.assertNull(val);
                                    return "ok";
                                })))
                        .collectList()
                        .then(Mono.from(conn.createStatement("DROP TABLE R2DBC_NULL_TBL").execute()))
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.close()))
        ));
    }

    @Test
    @Order(11)
    @DisplayName("11. bindNull 参数")
    void bindNull() {
        block(connect().flatMap(conn ->
                Mono.from(conn.createStatement(
                                "CREATE TABLE R2DBC_BN_TBL (ID NUMBER(10), NAME VARCHAR2(50))")
                        .execute())
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.createStatement("INSERT INTO R2DBC_BN_TBL VALUES (:id, :name)")
                                        .bind("id", 42)
                                        .bindNull("name", String.class)
                                        .execute())
                                .flatMap(r -> Mono.from(r.getRowsUpdated())))
                        .thenMany(Flux.from(conn.createStatement("SELECT NAME FROM R2DBC_BN_TBL WHERE ID = 42").execute())
                                .flatMap(r -> r.map((row, m) -> {
                                    String val = row.get("NAME", String.class);
                                    System.out.println("[BIND NULL] NAME = " + val);
                                    Assertions.assertNull(val);
                                    return "ok";
                                })))
                        .collectList()
                        .then(Mono.from(conn.createStatement("DROP TABLE R2DBC_BN_TBL").execute()))
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.close()))
        ));
    }

    @Test
    @Order(12)
    @DisplayName("12. Statement.add() 批量插入（JDBC batch 路径）")
    void statementAddBatch() {
        block(connect().flatMap(conn ->
                Mono.from(conn.createStatement(
                                "CREATE TABLE R2DBC_STMT_BATCH_TBL (ID NUMBER(10), NAME VARCHAR2(100))")
                        .execute())
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        // 使用 Statement.add() 一次性批量插入 5 行
                        .then(
                                Flux.from(conn.createStatement("INSERT INTO R2DBC_STMT_BATCH_TBL VALUES (:id, :name)")
                                                .bind("id", 1).bind("name", "alpha").add()
                                                .bind("id", 2).bind("name", "beta").add()
                                                .bind("id", 3).bind("name", "gamma").add()
                                                .bind("id", 4).bind("name", "delta").add()
                                                .bind("id", 5).bind("name", "epsilon").add()
                                                .execute())
                                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                                        .doOnNext(n -> System.out.println("[STMT BATCH INSERT] rowsUpdated=" + n))
                                        .collectList()
                                        .doOnNext(counts -> {
                                            System.out.println("[STMT BATCH] update counts = " + counts);
                                            Assertions.assertEquals(5, counts.size(),
                                                    "Expected one Result per batch entry");
                                            counts.forEach(c -> Assertions.assertEquals(1L, (long) c,
                                                    "Each batch entry should report 1 row updated"));
                                        })
                        )
                        // 验证全部 5 行数据已落库
                        .thenMany(Flux.from(conn.createStatement(
                                        "SELECT COUNT(*) AS CNT FROM R2DBC_STMT_BATCH_TBL").execute())
                                .flatMap(r -> r.map((row, m) -> row.get("CNT", Integer.class))))
                        .collectList()
                        .doOnNext(c -> {
                            System.out.println("[STMT BATCH COUNT] " + c);
                            Assertions.assertEquals(List.of(5), c);
                        })
                        // 验证行内容（按 ID 排序）
                        .thenMany(Flux.from(conn.createStatement(
                                        "SELECT ID, NAME FROM R2DBC_STMT_BATCH_TBL ORDER BY ID").execute())
                                .flatMap(r -> r.map((row, m) -> row.get("NAME", String.class))))
                        .collectList()
                        .doOnNext(names -> {
                            System.out.println("[STMT BATCH NAMES] " + names);
                            Assertions.assertEquals(
                                    List.of("alpha", "beta", "gamma", "delta", "epsilon"), names);
                        })
                        .then(Mono.from(conn.createStatement("DROP TABLE R2DBC_STMT_BATCH_TBL").execute()))
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.close()))
        ));
    }

    @Test
    @Order(13)
    @DisplayName("13. Batch 执行")
    void batch() {
        block(connect().flatMap(conn ->
                Mono.from(conn.createStatement(
                                "CREATE TABLE R2DBC_BATCH_TBL (ID NUMBER(10))")
                        .execute())
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(
                                Flux.from(conn.createBatch()
                                        .add("INSERT INTO R2DBC_BATCH_TBL VALUES (1)")
                                        .add("INSERT INTO R2DBC_BATCH_TBL VALUES (2)")
                                        .add("INSERT INTO R2DBC_BATCH_TBL VALUES (3)")
                                        .execute())
                                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                                        .doOnNext(n -> System.out.println("[BATCH] rowsUpdated=" + n))
                                        .then()
                        )
                        .thenMany(Flux.from(conn.createStatement("SELECT COUNT(*) AS CNT FROM R2DBC_BATCH_TBL").execute())
                                .flatMap(r -> r.map((row, m) -> row.get("CNT", Integer.class))))
                        .collectList()
                        .doOnNext(c -> {
                            System.out.println("[BATCH COUNT] " + c);
                            Assertions.assertEquals(List.of(3), c);
                        })
                        .then(Mono.from(conn.createStatement("DROP TABLE R2DBC_BATCH_TBL").execute()))
                        .flatMap(r -> Mono.from(r.getRowsUpdated()))
                        .then(Mono.from(conn.close()))
        ));
    }
}
