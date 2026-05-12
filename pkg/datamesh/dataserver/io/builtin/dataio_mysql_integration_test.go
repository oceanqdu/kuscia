//go:build integration
// +build integration

// Copyright 2024 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package builtin

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/secretflow/kuscia/pkg/common"
	"github.com/secretflow/kuscia/pkg/datamesh/dataserver/utils"
	pbv1alpha1 "github.com/secretflow/kuscia/proto/api/v1alpha1"
	"github.com/secretflow/kuscia/proto/api/v1alpha1/datamesh"
)

// ============================================================
// 测试配置 - 通过环境变量或默认值连接崖山数据库
// ============================================================

const (
	defaultYashanHost     = "172.28.61.143"
	defaultYashanPort     = "1690"
	defaultYashanUser     = "sales"
	defaultYashanPassword = "Swxa_2026"
	defaultYashanDatabase = "test"
)

func getYashanDSN() string {
	host := getEnv("YASHAN_HOST", defaultYashanHost)
	port := getEnv("YASHAN_PORT", defaultYashanPort)
	user := getEnv("YASHAN_USER", defaultYashanUser)
	pass := getEnv("YASHAN_PASSWORD", defaultYashanPassword)
	db := getEnv("YASHAN_DATABASE", defaultYashanDatabase)
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?timeout=10s&readTimeout=30s&writeTimeout=30s", user, pass, host, port, db)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func newYashanDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", getYashanDSN())
	require.NoError(t, err, "连接崖山数据库失败")
	db.SetConnMaxLifetime(30 * time.Second)
	require.NoError(t, db.Ping(), "Ping崖山数据库失败")
	return db
}

func cleanupTable(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	_, err := db.Exec("DROP TABLE IF EXISTS " + tableName)
	if err != nil {
		t.Logf("清理表 %s 失败: %v", tableName, err)
	}
	time.Sleep(200 * time.Millisecond)
}

// ============================================================
// 1. 连接层测试
// ============================================================

// T-C01: 连接崖山数据库
func TestYashan_Connection(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	assert.NoError(t, db.Ping())
}

// T-C02: 连接池复用
func TestYashan_ConnectionPool(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()

	for i := 0; i < 5; i++ {
		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		assert.NoError(t, err)
		assert.Equal(t, 1, result)
	}
}

// T-C03: 版本查询
func TestYashan_Version(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()

	var version string
	err := db.QueryRow("SELECT VERSION()").Scan(&version)
	assert.NoError(t, err)
	t.Logf("崖山数据库版本: %s", version)
	assert.NotEmpty(t, version)
}

// ============================================================
// 2. 读取层测试 (Downloader)
// ============================================================

// T-R01: 全类型读取 - 覆盖所有 Kuscia 支持的数据类型
func TestYashan_ReadAllTypes(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_read_all_types"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (
		c_bool BOOLEAN,
		c_int8 TINYINT,
		c_int16 SMALLINT,
		c_int32 INT,
		c_int64 BIGINT,
		c_uint8 TINYINT UNSIGNED,
		c_uint16 SMALLINT UNSIGNED,
		c_uint32 INT UNSIGNED,
		c_uint64 BIGINT UNSIGNED,
		c_float32 FLOAT,
		c_float64 DOUBLE,
		c_varchar VARCHAR(255),
		c_text TEXT,
		c_date DATE,
		c_timestamp TIMESTAMP,
		c_decimal DECIMAL(10,2)
	)`, tableName))
	require.NoError(t, err, "创建表失败")
	defer cleanupTable(t, db, tableName)

	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (
		true, 127, 32767, 2147483647, 9223372036854775807,
		255, 65535, 4294967295, 18446744073709551615,
		1.5, 2.718281828, 'hello', 'world text',
		'2026-05-11', '2026-05-11 10:30:00', 12345.67
	)`, tableName))
	require.NoError(t, err, "插入数据失败")

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Len(t, cols, 16, "列数不匹配")
	t.Logf("列名: %v", cols)

	assert.True(t, rows.Next(), "应该有一行数据")

	var (
		cBool       bool
		cInt8       int8
		cInt16      int16
		cInt32      int32
		cInt64      int64
		cUint8      uint8
		cUint16     uint16
		cUint32     uint32
		cUint64     uint64
		cFloat32    float32
		cFloat64    float64
		cVarchar    string
		cText       string
		cDate       []byte
		cTimestamp  []byte
		cDecimal    []byte
	)
	err = rows.Scan(&cBool, &cInt8, &cInt16, &cInt32, &cInt64,
		&cUint8, &cUint16, &cUint32, &cUint64,
		&cFloat32, &cFloat64, &cVarchar, &cText,
		&cDate, &cTimestamp, &cDecimal)
	assert.NoError(t, err, "扫描行数据失败")

	assert.True(t, cBool)
	assert.Equal(t, int8(127), cInt8)
	assert.Equal(t, int16(32767), cInt16)
	assert.Equal(t, int32(2147483647), cInt32)
	assert.Equal(t, int64(9223372036854775807), cInt64)
	assert.Equal(t, uint8(255), cUint8)
	assert.Equal(t, uint16(65535), cUint16)
	assert.Equal(t, uint32(4294967295), cUint32)
	assert.Equal(t, uint64(18446744073709551615), cUint64)
	assert.InDelta(t, float32(1.5), cFloat32, 0.001)
	assert.InDelta(t, 2.718281828, cFloat64, 0.000001)
	assert.Equal(t, "hello", cVarchar)
	assert.Equal(t, "world text", cText)
	assert.Equal(t, "2026-05-11", string(cDate))
	assert.Contains(t, string(cTimestamp), "2026-05-11")
	assert.Equal(t, "12345.67", string(cDecimal))
}

// T-R02: 反引号表名/列名读取
func TestYashan_ReadWithBacktickNames(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_backtick_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (` + "`" + `my col` + "`" + ` INT, ` + "`" + `name` + "`" + ` VARCHAR(100))", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (42, 'test')", tableName))
	require.NoError(t, err)

	var col1 int
	var col2 string
	err = db.QueryRow(fmt.Sprintf("SELECT ` + "`" + `my col` + "`" + `, ` + "`" + `name` + "`" + ` FROM %s", tableName)).Scan(&col1, &col2)
	assert.NoError(t, err)
	assert.Equal(t, 42, col1)
	assert.Equal(t, "test", col2)
}

// T-R03: 表名大小写不敏感
func TestYashan_ReadTableNameCaseInsensitive(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_case_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT)", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1)", tableName))
	require.NoError(t, err)

	var cnt int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 1, cnt)

	err = db.QueryRow("SELECT COUNT(*) FROM KUSCIA_CASE_TEST").Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 1, cnt)
}

// T-R04: NULL 值读取
func TestYashan_ReadNullValues(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_null_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, name VARCHAR(100))", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, NULL)", tableName))
	require.NoError(t, err)

	rows, err := db.Query(fmt.Sprintf("SELECT id, name FROM %s", tableName))
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var id int
	var name sql.NullString
	err = rows.Scan(&id, &name)
	assert.NoError(t, err)
	assert.Equal(t, 1, id)
	assert.False(t, name.Valid, "name 应该是 NULL")
}

// T-R05: 空结果集读取
func TestYashan_ReadEmptyResult(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_empty_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT)", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s WHERE id = 999", tableName))
	require.NoError(t, err)
	defer rows.Close()

	assert.False(t, rows.Next(), "应该没有数据")
}

// T-R06: 带 WHERE 条件查询
func TestYashan_ReadWithWhere(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_where_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, val VARCHAR(50))", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	for i := 1; i <= 5; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (%d, 'val%d')", tableName, i, i))
		require.NoError(t, err)
	}

	rows, err := db.Query(fmt.Sprintf("SELECT id, val FROM %s WHERE id > 3", tableName))
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var val string
		rows.Scan(&id, &val)
		count++
	}
	assert.Equal(t, 2, count, "应该有2行 id > 3")
}

// T-R07: 批量读取（1000+ 行）
func TestYashan_ReadBatch(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_batch_read_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, val DOUBLE)", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	tx, err := db.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName))
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		_, err = stmt.Exec(i, float64(i)*1.1)
		require.NoError(t, err)
	}
	stmt.Close()
	require.NoError(t, tx.Commit())

	rows, err := db.Query(fmt.Sprintf("SELECT id, val FROM %s ORDER BY id", tableName))
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var val float64
		rows.Scan(&id, &val)
		if count < 3 {
			t.Logf("行 %d: id=%d, val=%f", count, id, val)
		}
		count++
	}
	assert.Equal(t, 1000, count, "应该读取1000行")
}

// ============================================================
// 3. 写入层测试 (Uploader)
// ============================================================

// T-W01: 创建表 (CREATE TABLE)
func TestYashan_WriteCreateTable(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_create_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, name TEXT, score DOUBLE)", tableName))
	assert.NoError(t, err, "CREATE TABLE 失败")
	defer cleanupTable(t, db, tableName)

	var cnt int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 0, cnt)
}

// T-W02: DROP TABLE IF EXISTS
func TestYashan_WriteDropTable(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_drop_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT)", tableName))
	require.NoError(t, err)

	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	assert.NoError(t, err, "DROP TABLE 失败")

	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	assert.NoError(t, err)
}

// T-W03: INSERT 单行
func TestYashan_WriteInsertSingle(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_insert_single_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, name VARCHAR(100))", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'alice')", tableName))
	assert.NoError(t, err, "INSERT 失败")

	var name string
	err = db.QueryRow(fmt.Sprintf("SELECT name FROM %s WHERE id = 1", tableName)).Scan(&name)
	assert.NoError(t, err)
	assert.Equal(t, "alice", name)
}

// T-W04: INSERT 批量（事务）
func TestYashan_WriteInsertBatch(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_insert_batch_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, val DOUBLE)", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	tx, err := db.Begin()
	require.NoError(t, err)

	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName))
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err = stmt.Exec(i, float64(i)*0.5)
		require.NoError(t, err)
	}
	stmt.Close()

	require.NoError(t, tx.Commit())

	var cnt int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 100, cnt)
}

// T-W05: 事务 COMMIT
func TestYashan_WriteTransactionCommit(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_tx_commit_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, val VARCHAR(50))", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'commit_test')", tableName))
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	var val string
	err = db.QueryRow(fmt.Sprintf("SELECT val FROM %s WHERE id = 1", tableName)).Scan(&val)
	assert.NoError(t, err)
	assert.Equal(t, "commit_test", val)
}

// T-W06: 事务 ROLLBACK
func TestYashan_WriteTransactionRollback(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_tx_rollback_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, val VARCHAR(50))", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'before')", tableName))
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'rollback_test')", tableName))
	require.NoError(t, err)

	require.NoError(t, tx.Rollback())

	var cnt int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 1, cnt, "ROLLBACK 后应该只有1行")
}

// T-W07: DELETE FROM 降级清理
func TestYashan_WriteDeleteFrom(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_delete_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT)", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1), (2), (3)", tableName))
	require.NoError(t, err)

	_, err = db.Exec(fmt.Sprintf("DELETE FROM %s", tableName))
	assert.NoError(t, err, "DELETE FROM 失败")

	var cnt int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 0, cnt)
}

// ============================================================
// 4. 数据类型映射测试
// ============================================================

// T-T01: Kuscia Arrow→MySQL 类型映射在崖山上验证
func TestYashan_TypeMapping_ArrowToMySQL(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_type_mapping_test"
	cleanupTable(t, db, tableName)

	uploader := &MySQLUploader{}
	fields := []struct {
		name string
		typ  arrow.DataType
	}{
		{"c_bool", arrow.FixedWidthTypes.Boolean},
		{"c_int8", arrow.PrimitiveTypes.Int8},
		{"c_int16", arrow.PrimitiveTypes.Int16},
		{"c_int32", arrow.PrimitiveTypes.Int32},
		{"c_int64", arrow.PrimitiveTypes.Int64},
		{"c_uint8", arrow.PrimitiveTypes.Uint8},
		{"c_uint16", arrow.PrimitiveTypes.Uint16},
		{"c_uint32", arrow.PrimitiveTypes.Uint32},
		{"c_uint64", arrow.PrimitiveTypes.Uint64},
		{"c_float32", arrow.PrimitiveTypes.Float32},
		{"c_float64", arrow.PrimitiveTypes.Float64},
		{"c_string", arrow.BinaryTypes.String},
		{"c_date", arrow.PrimitiveTypes.Date32},
		{"c_datetime", arrow.PrimitiveTypes.Date64},
		{"c_time", arrow.FixedWidthTypes.Time32s},
		{"c_timestamp", arrow.FixedWidthTypes.Timestamp_s},
	}

	sql := "CREATE TABLE " + tableName + " ("
	for i, f := range fields {
		if i > 0 {
			sql += ", "
		}
		mysqlType := uploader.ArrowDataTypeToMySQLType(f.typ)
		sql += "`" + f.name + "` " + mysqlType
		t.Logf("映射: %s (%T) → %s", f.name, f.typ, mysqlType)
	}
	sql += ")"

	_, err := db.Exec(sql)
	assert.NoError(t, err, "CREATE TABLE 使用 Kuscia 类型映射失败")
	defer cleanupTable(t, db, tableName)

	t.Log("所有 Kuscia Arrow→MySQL 类型映射在崖山上创建成功")
}

// T-T02: MySQL→Arrow 类型转换在崖山上验证
func TestYashan_TypeMapping_MySQLToArrow(t *testing.T) {
	testCases := []struct {
		kusciaType string
		mysqlType  string
	}{
		{"int8", "TINYINT"},
		{"int16", "SMALLINT"},
		{"int32", "INT"},
		{"int64", "BIGINT"},
		{"float32", "FLOAT"},
		{"float64", "DOUBLE"},
		{"string", "VARCHAR(100)"},
		{"date", "DATE"},
		{"timestamp", "TIMESTAMP"},
		{"bool", "BOOLEAN"},
	}

	for _, tc := range testCases {
		t.Run(tc.kusciaType, func(t *testing.T) {
			arrowType := common.Convert2ArrowColumnType(tc.kusciaType)
			assert.NotNil(t, arrowType, "Kuscia 类型 %s 无法转换为 Arrow 类型", tc.kusciaType)
			t.Logf("Kuscia 类型 %s → Arrow %s", tc.kusciaType, arrowType.Name())
		})
	}
}

// ============================================================
// 5. 集成测试: Downloader 完整流程
// ============================================================

// T-I01: 使用 Kuscia MySQLDownloader 从崖山读取数据
func TestYashan_Integration_Downloader(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_downloader_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (name VARCHAR(100), id INT, score DOUBLE)", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ('alice', 1, 95.5), ('bob', 2, 87.3), ('carol', 3, 92.1)", tableName))
	require.NoError(t, err)

	dd := &datamesh.DomainData{
		DomaindataId: "test-download",
		RelativeUri:  tableName,
		Columns: []*pbv1alpha1.DataColumn{
			{Name: "name", Type: "string"},
			{Name: "id", Type: "int32"},
			{Name: "score", Type: "float64"},
		},
	}

	query := &datamesh.CommandDomainDataQuery{
		DomaindataId: "test-download",
	}

	downloader := NewMySQLDownloader(context.Background(), db, dd, query)

	mgs := &mockDoGetServer{
		ServerStream: &mockGrpcServerStream{},
	}
	schema, err := utils.GenerateArrowSchema(dd)
	require.NoError(t, err)
	writer := flight.NewRecordWriter(mgs, ipc.WithSchema(schema))

	err = downloader.DataProxyContentToFlightStreamSQL(writer)
	assert.NoError(t, err, "Downloader 从崖山读取失败")
	assert.NotEmpty(t, mgs.dataList, "应该有数据返回")
	t.Logf("Downloader 返回 %d 个 FlightData", len(mgs.dataList))
}

// T-I02: 使用 Kuscia MySQLUploader 向崖山写入数据
func TestYashan_Integration_Uploader(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_uploader_test"
	cleanupTable(t, db, tableName)

	dd := &datamesh.DomainData{
		DomaindataId: "test-upload",
		RelativeUri:  tableName,
		Columns: []*pbv1alpha1.DataColumn{
			{Name: "name", Type: "string"},
			{Name: "id", Type: "int64"},
			{Name: "score", Type: "float64"},
		},
	}

	query := &datamesh.CommandDomainDataQuery{
		DomaindataId: "test-upload",
	}

	colTypes := []arrow.DataType{
		arrow.BinaryTypes.String,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float64,
	}
	dataRows := [][]any{
		{"alice", int64(1), float64(95.5)},
		{"bob", int64(2), float64(87.3)},
	}

	mgs := &mockDoGetServer{
		ServerStream: &mockGrpcServerStream{},
	}
	schema, err := utils.GenerateArrowSchema(dd)
	require.NoError(t, err)
	writer := flight.NewRecordWriter(mgs, ipc.WithSchema(schema))

	recordBuilder := make([]array.Builder, len(colTypes))
	recordBuilder[0] = array.NewStringBuilder(memory.DefaultAllocator)
	recordBuilder[1] = array.NewInt64Builder(memory.DefaultAllocator)
	recordBuilder[2] = array.NewFloat64Builder(memory.DefaultAllocator)

	for _, row := range dataRows {
		recordBuilder[0].(*array.StringBuilder).Append(row[0].(string))
		recordBuilder[1].(*array.Int64Builder).Append(row[1].(int64))
		recordBuilder[2].(*array.Float64Builder).Append(row[2].(float64))
	}

	recordData := make([]arrow.Array, len(colTypes))
	for i, b := range recordBuilder {
		recordData[i] = b.NewArray()
	}
	require.NoError(t, writer.Write(array.NewRecord(schema, recordData, int64(len(dataRows)))))
	writer.Close()

	reader, err := flight.NewRecordReader(&mockDoPutServer{
		ServerStream: &mockGrpcServerStream{},
		nextDataList: mgs.dataList,
	})
	require.NoError(t, err)

	uploader := NewMySQLUploader(context.Background(), db, dd, query)
	err = uploader.FlightStreamToDataProxyContentMySQL(reader)
	assert.NoError(t, err, "Uploader 向崖山写入失败")

	var cnt int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 2, cnt, "应该有2行数据")

	var name string
	var id int64
	var score float64
	err = db.QueryRow(fmt.Sprintf("SELECT name, id, score FROM %s WHERE id = 1", tableName)).Scan(&name, &id, &score)
	assert.NoError(t, err)
	assert.Equal(t, "alice", name)
	assert.Equal(t, int64(1), id)
	assert.InDelta(t, 95.5, score, 0.01)

	t.Log("Uploader 向崖山写入并验证成功")
}

// T-I03: 读写往返测试 (Read-Write Roundtrip)
func TestYashan_Integration_ReadWriteRoundtrip(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()

	srcTable := "kuscia_roundtrip_src"
	dstTable := "kuscia_roundtrip_dst"
	cleanupTable(t, db, srcTable)
	cleanupTable(t, db, dstTable)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (name VARCHAR(100), value INT)", srcTable))
	require.NoError(t, err)
	defer cleanupTable(t, db, srcTable)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ('a', 10), ('b', 20), ('c', 30)", srcTable))
	require.NoError(t, err)

	srcDD := &datamesh.DomainData{
		DomaindataId: "roundtrip-src",
		RelativeUri:  srcTable,
		Columns: []*pbv1alpha1.DataColumn{
			{Name: "name", Type: "string"},
			{Name: "value", Type: "int64"},
		},
	}
	srcQuery := &datamesh.CommandDomainDataQuery{DomaindataId: "roundtrip-src"}
	downloader := NewMySQLDownloader(context.Background(), db, srcDD, srcQuery)

	mgs := &mockDoGetServer{ServerStream: &mockGrpcServerStream{}}
	schema, err := utils.GenerateArrowSchema(srcDD)
	require.NoError(t, err)
	writer := flight.NewRecordWriter(mgs, ipc.WithSchema(schema))

	err = downloader.DataProxyContentToFlightStreamSQL(writer)
	require.NoError(t, err, "读取源表失败")
	require.NotEmpty(t, mgs.dataList)

	dstDD := &datamesh.DomainData{
		DomaindataId: "roundtrip-dst",
		RelativeUri:  dstTable,
		Columns: []*pbv1alpha1.DataColumn{
			{Name: "name", Type: "string"},
			{Name: "value", Type: "int64"},
		},
	}
	dstQuery := &datamesh.CommandDomainDataQuery{DomaindataId: "roundtrip-dst"}

	reader, err := flight.NewRecordReader(&mockDoPutServer{
		ServerStream: &mockGrpcServerStream{},
		nextDataList: mgs.dataList,
	})
	require.NoError(t, err)

	uploader := NewMySQLUploader(context.Background(), db, dstDD, dstQuery)
	err = uploader.FlightStreamToDataProxyContentMySQL(reader)
	require.NoError(t, err, "写入目标表失败")

	var cnt int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 3, cnt, "目标表应该有3行")

	t.Log("读写往返测试通过")
}

// ============================================================
// 补充测试用例
// ============================================================

// T-R08: 部分列读取
func TestYashan_ReadPartialColumn(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_partial_col_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (name VARCHAR(100), id INT, score DOUBLE)", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ('alice', 1, 95.5), ('bob', 2, 87.3)", tableName))
	require.NoError(t, err)

	rows, err := db.Query(fmt.Sprintf("SELECT ` + "`" + `id` + "`" + ` FROM %s", tableName))
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		assert.NoError(t, err)
		count++
	}
	assert.Equal(t, 2, count, "应该读取到 2 行")
	t.Log("部分列读取测试通过")
}

// T-W08: DELETE FROM 降级清理
func TestYashan_WriteDeleteFallback(t *testing.T) {
	db := newYashanDB(t)
	defer db.Close()
	tableName := "kuscia_delete_fallback_test"
	cleanupTable(t, db, tableName)

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, val VARCHAR(50))", tableName))
	require.NoError(t, err)
	defer cleanupTable(t, db, tableName)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName))
	require.NoError(t, err)

	var cnt int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&cnt)
	require.NoError(t, err)
	require.Equal(t, 3, cnt)

	_, dropErr := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if dropErr != nil {
		t.Logf("DROP 失败: %v, 尝试 DELETE", dropErr)
		_, deleteErr := db.Exec(fmt.Sprintf("DELETE FROM %s", tableName))
		assert.NoError(t, deleteErr, "DELETE 降级应该成功")
	}

	if dropErr == nil {
		_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, val VARCHAR(50))", tableName))
		assert.NoError(t, err, "重新建表应该成功")
	}

	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 0, cnt, "表应该为空")

	t.Log("DELETE 降级清理测试通过")
}
