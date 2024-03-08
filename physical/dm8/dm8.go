// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package dm8

import (
	"context"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/go-secure-stdlib/strutil"
	"github.com/hashicorp/vault/sdk/physical"

	// "net/url"
	_ "dm"
)

// Verify DMBackend satisfies the correct interfaces
var (
	_ physical.Backend   = (*DMBackend)(nil)
	_ physical.HABackend = (*DMBackend)(nil)
	_ physical.Lock      = (*DMHALock)(nil)
)

// Unreserved tls key
// Reserved values are "true", "false", "skip-verify"
const dmTLSKey = "default"

// DMBackend is a physical backend that stores data
// within DM database.
type DMBackend struct {
	dbTable      string
	dbLockTable  string
	client       *sql.DB
	statements   map[string]*sql.Stmt
	logger       log.Logger
	permitPool   *physical.PermitPool
	conf         map[string]string
	redirectHost string
	redirectPort int64
	haEnabled    bool
}

// Function to find the index of the last occurrence of a byte in a byte slice
func lastIndexOfByte(slice []byte, b byte) int {
	for i := len(slice) - 1; i >= 0; i-- {
		if slice[i] == b {
			return i
		}
	}
	return -1
}

// NewDMBackend constructs a DM backend using the given API client and
// server address and credential for accessing dm database.
func NewDMBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	var err error

	db, err := NewDMClient(conf, logger)
	if err != nil {
		return nil, err
	}

	database := conf["database"]
	if database == "" {
		database = "vault"
	}
	table := conf["table"]
	if table == "" {
		table = "vault"
	}

	err = validateDBTable(database, table)
	if err != nil {
		return nil, err
	}

	dbTable := fmt.Sprintf("\"%s\".\"%s\"", database, table)

	maxParStr, ok := conf["max_parallel"]
	var maxParInt int
	if ok {
		maxParInt, err = strconv.Atoi(maxParStr)
		if err != nil {
			return nil, fmt.Errorf("failed parsing max_parallel parameter: %w", err)
		}
		if logger.IsDebug() {
			logger.Debug("max_parallel set", "max_parallel", maxParInt)
		}
	} else {
		maxParInt = physical.DefaultParallelOperations
	}

	// Check schema exists
	var schemaExist int
	schemaRows, err := db.Query("select COUNT(1) from sysobjects where NAME=? and SUBTYPE$ is null", database)
	if err != nil {
		return nil, fmt.Errorf("failed to check dm schema exist: %w", err)
	}
	defer schemaRows.Close()
	schemaRows.Next()
	schemaRows.Scan(&schemaExist)

	// Create the required database if it doesn't exists.
	if schemaExist == 0 {
		// Get database file path
		sql := fmt.Sprintf("CREATE SCHEMA \"%s\"", database)
		if _, err := db.Exec(sql); err != nil {
			return nil, fmt.Errorf("failed to create dm database: %w", err)
		}
	}

	// Check table exists
	var tableExist int
	tableRows, err := db.Query("SELECT COUNT(*)  FROM all_tables WHERE OWNER = ? AND TABLE_NAME = ?", database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to check dm table exist: %w", err)
	}
	defer tableRows.Close()
	tableRows.Next()
	tableRows.Scan(&tableExist)

	// Create the required table if it doesn't exists.
	if tableExist == 0 {
		// d.logger.Debug("dbTable: ", dbTable)
		sql := fmt.Sprintf("CREATE TABLE %s (vault_key varchar(3072), vault_value blob, PRIMARY KEY (vault_key))", dbTable)
		if _, err := db.Exec(sql); err != nil {
			return nil, fmt.Errorf("failed to create dm table: %w", err)
		}
	}

	// Default value for ha_enabled
	haEnabledStr, ok := conf["ha_enabled"]
	if !ok {
		haEnabledStr = "false"
	}
	haEnabled, err := strconv.ParseBool(haEnabledStr)
	if err != nil {
		return nil, fmt.Errorf("value [%v] of 'ha_enabled' could not be understood", haEnabledStr)
	}

	locktable, ok := conf["lock_table"]
	if !ok {
		locktable = table + "_lock"
	}

	dbLockTable := "\"" + database + "\".\"" + locktable + "\""

	// Only create lock table if ha_enabled is true
	if haEnabled {
		// Check table exists
		var lockTableExist bool
		lockTableRows, err := db.Query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?", locktable, database)
		if err != nil {
			return nil, fmt.Errorf("failed to check dm table exist: %w", err)
		}
		defer lockTableRows.Close()
		lockTableExist = lockTableRows.Next()

		// Create the required table if it doesn't exists.
		if !lockTableExist {
			create_query := "CREATE TABLE " + dbLockTable +
				" (node_job varbinary(512), current_leader varbinary(512), PRIMARY KEY (node_job))"
			if _, err := db.Exec(create_query); err != nil {
				return nil, fmt.Errorf("failed to create dm table: %w", err)
			}
		}
	}

	// Setup the backend.
	dmb := &DMBackend{
		dbTable:     dbTable,
		dbLockTable: dbLockTable,
		client:      db,
		statements:  make(map[string]*sql.Stmt),
		logger:      logger,
		permitPool:  physical.NewPermitPool(maxParInt),
		conf:        conf,
		haEnabled:   haEnabled,
	}

	// Prepare all the statements required
	statements := map[string]string{
		// "put": "INSERT INTO " + dbTable + " VALUES( ?, ? ) ON DUPLICATE KEY UPDATE vault_value=VALUES(vault_value)",
		"get":    "SELECT COALESCE(vault_value, '') AS vault_value FROM " + dbTable + " WHERE vault_key = ?",
		"delete": "DELETE FROM " + dbTable + " WHERE vault_key = ?",
		"list":   "SELECT vault_key FROM " + dbTable + " WHERE vault_key LIKE ?",
		"insert": "INSERT INTO " + dbTable + " VALUES( ?, ?)",
		"update": "UPDATE " + dbTable + " SET vault_value=? WHERE vault_key=?",
	}

	// Only prepare ha-related statements if we need them
	if haEnabled {
		statements["get_lock"] = "SELECT current_leader FROM " + dbLockTable + " WHERE node_job = ?"
		statements["used_lock"] = "SELECT IS_USED_LOCK(?)"
	}

	for name, query := range statements {
		if err := dmb.prepare(name, query); err != nil {
			return nil, err
		}
	}

	return dmb, nil
}

// validateDBTable to prevent SQL injection attacks. This ensures that the database and table names only have valid
// characters in them. DM allows for more characters that this will allow, but there isn't an easy way of
// representing the full Unicode Basic Multilingual Plane to check against.
func validateDBTable(db, table string) (err error) {
	merr := &multierror.Error{}
	merr = multierror.Append(merr, wrapErr("invalid database: %w", validate(db)))
	merr = multierror.Append(merr, wrapErr("invalid table: %w", validate(table)))
	return merr.ErrorOrNil()
}

func validate(name string) (err error) {
	if name == "" {
		return fmt.Errorf("missing name")
	}
	// - Permitted characters in quoted identifiers include the full Unicode Basic Multilingual Plane (BMP), except U+0000:
	//    ASCII: U+0001 .. U+007F
	//    Extended: U+0080 .. U+FFFF
	// - ASCII NUL (U+0000) and supplementary characters (U+10000 and higher) are not permitted in quoted or unquoted identifiers.
	// - Identifiers may begin with a digit but unless quoted may not consist solely of digits.
	// - Database, table, and column names cannot end with space characters.
	//
	// We are explicitly excluding all space characters (it's easier to deal with)
	// The name will be quoted, so the all-digit requirement doesn't apply
	runes := []rune(name)
	validationErr := fmt.Errorf("invalid character found: can only include printable, non-space characters between [0x0001-0xFFFF]")
	for _, r := range runes {
		// U+0000 Explicitly disallowed
		if r == 0x0000 {
			return fmt.Errorf("invalid character: cannot include 0x0000")
		}
		// Cannot be above 0xFFFF
		if r > 0xFFFF {
			return fmt.Errorf("invalid character: cannot include any characters above 0xFFFF")
		}
		if r == '`' {
			return fmt.Errorf("invalid character: cannot include '`' character")
		}
		if r == '\'' || r == '"' {
			return fmt.Errorf("invalid character: cannot include quotes")
		}
		// We are excluding non-printable characters (not mentioned in the docs)
		if !unicode.IsPrint(r) {
			return validationErr
		}
		// We are excluding space characters (not mentioned in the docs)
		if unicode.IsSpace(r) {
			return validationErr
		}
	}
	return nil
}

func wrapErr(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(message, err)
}

func NewDMClient(conf map[string]string, logger log.Logger) (*sql.DB, error) {
	var err error

	// Get the MySQL credentials to perform read/write operations.
	username, ok := conf["username"]
	if !ok || username == "" {
		return nil, fmt.Errorf("missing username")
	}
	password, ok := conf["password"]
	if !ok || password == "" {
		return nil, fmt.Errorf("missing password")
	}

	// Get or set DM server address. Defaults to localhost and default port(3306)
	address, ok := conf["address"]
	if !ok {
		address = "127.0.0.1:3306"
	}

	maxIdleConnStr, ok := conf["max_idle_connections"]
	var maxIdleConnInt int
	if ok {
		maxIdleConnInt, err = strconv.Atoi(maxIdleConnStr)
		if err != nil {
			return nil, fmt.Errorf("failed parsing max_idle_connections parameter: %w", err)
		}
		if logger.IsDebug() {
			logger.Debug("max_idle_connections set", "max_idle_connections", maxIdleConnInt)
		}
	}

	maxConnLifeStr, ok := conf["max_connection_lifetime"]
	var maxConnLifeInt int
	if ok {
		maxConnLifeInt, err = strconv.Atoi(maxConnLifeStr)
		if err != nil {
			return nil, fmt.Errorf("failed parsing max_connection_lifetime parameter: %w", err)
		}
		if logger.IsDebug() {
			logger.Debug("max_connection_lifetime set", "max_connection_lifetime", maxConnLifeInt)
		}
	}

	maxParStr, ok := conf["max_parallel"]
	var maxParInt int
	if ok {
		maxParInt, err = strconv.Atoi(maxParStr)
		if err != nil {
			return nil, fmt.Errorf("failed parsing max_parallel parameter: %w", err)
		}
		if logger.IsDebug() {
			logger.Debug("max_parallel set", "max_parallel", maxParInt)
		}
	} else {
		maxParInt = physical.DefaultParallelOperations
	}

	// 	dsnParams := url.Values{}
	// 	tlsCaFile, tlsOk := conf["tls_ca_file"]
	// 	if tlsOk {
	// 		if err := setupDMTLSConfig(tlsCaFile); err != nil {
	// 			return nil, fmt.Errorf("failed register TLS config: %w", err)
	// 		}

	// 		dsnParams.Add("tls", dmTLSKey)
	// 	}
	// 	ptAllowed, ptOk := conf["plaintext_connection_allowed"]
	// 	if !(ptOk && strings.ToLower(ptAllowed) == "true") && !tlsOk {
	// 		logger.Warn("No TLS specified, credentials will be sent in plaintext. To mute this warning add 'plaintext_connection_allowed' with a true value to your MySQL configuration in your config file.")
	// 	}

	dsn := "dm://" + username + ":" + password + "@" + address
	// dsn := "dm://SYSDBA:SYSDBA@172.16.6.205:5236"
	var db *sql.DB

	if db, err = sql.Open("dm", dsn); err != nil {
		return nil, fmt.Errorf("failed to connecto dm: %w", err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping dm: %w", err)
	}

	db.SetMaxOpenConns(maxParInt)
	if maxIdleConnInt != 0 {
		db.SetMaxIdleConns(maxIdleConnInt)
	}
	if maxConnLifeInt != 0 {
		db.SetConnMaxLifetime(time.Duration(maxConnLifeInt) * time.Second)
	}

	return db, err
}

// prepare is a helper to prepare a query for future execution
func (dmb *DMBackend) prepare(name, query string) error {
	stmt, err := dmb.client.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare %q: %w", name, err)
	}
	dmb.statements[name] = stmt
	return nil
}

// Put is used to insert or update an entry.
// func (d *DMBackend) Put(ctx context.Context, entry *physical.Entry) error {
// 	d.logger.Debug("Put running")

// 	defer metrics.MeasureSince([]string{"dm", "put"}, time.Now())

// 	d.permitPool.Acquire()
// 	defer d.permitPool.Release()

// 	_, err := d.statements["put"].Exec(entry.Key, entry.Value)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (dmb *DMBackend) Put(ctx context.Context, entry *physical.Entry) error {
	defer metrics.MeasureSince([]string{"dm", "insert"}, time.Now())

	dmb.permitPool.Acquire()
	defer dmb.permitPool.Release()

	_, err := dmb.statements["insert"].Exec(entry.Key, entry.Value)
	if err != nil {
		defer metrics.MeasureSince([]string{"dm", "update"}, time.Now())
		_, err = dmb.statements["update"].Exec(entry.Value, entry.Key)
	}

	return err
}

// Get is used to fetch an entry.
func (dmb *DMBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	defer metrics.MeasureSince([]string{"dm", "get"}, time.Now())

	dmb.permitPool.Acquire()
	defer dmb.permitPool.Release()

	var result []byte
	err := dmb.statements["get"].QueryRow(key).Scan(&result)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	ent := &physical.Entry{
		Key:   key,
		Value: result,
	}

	return ent, nil
}

// Delete is used to permanently delete an entry
func (dmb *DMBackend) Delete(ctx context.Context, key string) error {
	defer metrics.MeasureSince([]string{"dm", "delete"}, time.Now())

	dmb.permitPool.Acquire()
	defer dmb.permitPool.Release()

	_, err := dmb.statements["delete"].Exec(key)
	if err != nil {
		return err
	}
	return nil
}

// List is used to list all the keys under a given
// prefix, up to the next prefix.
func (dmb *DMBackend) List(ctx context.Context, prefix string) ([]string, error) {
	defer metrics.MeasureSince([]string{"dm", "list"}, time.Now())

	dmb.permitPool.Acquire()
	defer dmb.permitPool.Release()

	likePrefix := prefix + "%"
	// sql := fmt.Sprintf("SELECT vault_value FROM %s WHERE vault_key like '%s'", d.dbTable, likePrefix)
	// rows, err := d.client.Query(sql)

	rows, err := dmb.statements["list"].Query(likePrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statement: %w", err)
	}
	// defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		err = rows.Scan(&key)
		if err != nil {
			return nil, fmt.Errorf("failed to scan rows: %w", err)
		}

		key = strings.TrimPrefix(key, prefix)
		if i := strings.Index(key, "/"); i == -1 {
			// Add objects only from the current 'folder'
			keys = append(keys, key)
		} else if i != -1 {
			// Add truncated 'folder' paths
			keys = strutil.AppendIfMissing(keys, string(key[:i+1]))
		}
	}

	sort.Strings(keys)
	return keys, nil
}

// LockWith is used for mutual exclusion based on the given key.
func (dmb *DMBackend) LockWith(key, value string) (physical.Lock, error) {
	l := &DMHALock{
		in:     dmb,
		key:    key,
		value:  value,
		logger: dmb.logger,
	}
	return l, nil
}

func (dmb *DMBackend) HAEnabled() bool {
	return dmb.haEnabled
}

// DMHALock is a DM Lock implementation for the HABackend
type DMHALock struct {
	in     *DMBackend
	key    string
	value  string
	logger log.Logger

	held      bool
	localLock sync.Mutex
	leaderCh  chan struct{}
	stopCh    <-chan struct{}
	lock      *DMLock
}

func (i *DMHALock) Lock(stopCh <-chan struct{}) (<-chan struct{}, error) {
	i.localLock.Lock()
	defer i.localLock.Unlock()
	if i.held {
		return nil, fmt.Errorf("lock already held")
	}

	// Attempt an async acquisition
	didLock := make(chan struct{})
	failLock := make(chan error, 1)
	releaseCh := make(chan bool, 1)
	go i.attemptLock(i.key, i.value, didLock, failLock, releaseCh)

	// Wait for lock acquisition, failure, or shutdown
	select {
	case <-didLock:
		releaseCh <- false
	case err := <-failLock:
		return nil, err
	case <-stopCh:
		releaseCh <- true
		return nil, nil
	}

	// Create the leader channel
	i.held = true
	i.leaderCh = make(chan struct{})

	go i.monitorLock(i.leaderCh)

	i.stopCh = stopCh

	return i.leaderCh, nil
}

func (i *DMHALock) attemptLock(key, value string, didLock chan struct{}, failLock chan error, releaseCh chan bool) {
	lock, err := NewDMLock(i.in, i.logger, key, value)
	if err != nil {
		failLock <- err
		return
	}

	// Set node value
	i.lock = lock

	err = lock.Lock()
	if err != nil {
		failLock <- err
		return
	}

	// Signal that lock is held
	close(didLock)

	// Handle an early abort
	release := <-releaseCh
	if release {
		lock.Unlock()
	}
}

func (i *DMHALock) monitorLock(leaderCh chan struct{}) {
	for {
		// The only way to lose this lock is if someone is
		// logging into the DB and altering system tables or you lose a connection in
		// which case you will lose the lock anyway.
		err := i.hasLock(i.key)
		if err != nil {
			// Somehow we lost the lock.... likely because the connection holding
			// the lock was closed or someone was playing around with the locks in the DB.
			close(leaderCh)
			return
		}

		time.Sleep(5 * time.Second)
	}
}

func (i *DMHALock) Unlock() error {
	i.localLock.Lock()
	defer i.localLock.Unlock()
	if !i.held {
		return nil
	}

	err := i.lock.Unlock()

	if err == nil {
		i.held = false
		return nil
	}

	return err
}

// hasLock will check if a lock is held by checking the current lock id against our known ID.
func (i *DMHALock) hasLock(key string) error {
	var result sql.NullInt64
	err := i.in.statements["used_lock"].QueryRow(key).Scan(&result)
	if err == sql.ErrNoRows || !result.Valid {
		// This is not an error to us since it just means the lock isn't held
		return nil
	}

	if err != nil {
		return err
	}

	// IS_USED_LOCK will return the ID of the connection that created the lock.
	if result.Int64 != GlobalLockID {
		return ErrLockHeld
	}

	return nil
}

func (i *DMHALock) GetLeader() (string, error) {
	defer metrics.MeasureSince([]string{"dm", "lock_get"}, time.Now())
	var result string
	err := i.in.statements["get_lock"].QueryRow("leader").Scan(&result)
	if err == sql.ErrNoRows {
		return "", err
	}

	return result, nil
}

func (i *DMHALock) Value() (bool, string, error) {
	leaderkey, err := i.GetLeader()
	if err != nil {
		return false, "", err
	}

	return true, leaderkey, err
}

// DMLock provides an easy way to grab and release dm
// locks using the built in GET_LOCK function. Note that these
// locks are released when you lose connection to the server.
type DMLock struct {
	parentConn *DMBackend
	in         *sql.DB
	logger     log.Logger
	statements map[string]*sql.Stmt
	key        string
	value      string
}

// Errors specific to trying to grab a lock in DM
var (
	// This is the GlobalLockID for checking if the lock we got is still the current lock
	GlobalLockID int64
	// ErrLockHeld is returned when another vault instance already has a lock held for the given key.
	ErrLockHeld = errors.New("dm: lock already held")
	// ErrUnlockFailed
	ErrUnlockFailed = errors.New("dm: unable to release lock, already released or not held by this session")
	// You were unable to update that you are the new leader in the DB
	ErrClaimFailed = errors.New("dm: unable to update DB with new leader information")
	// Error to throw if between getting the lock and checking the ID of it we lost it.
	ErrSettingGlobalID = errors.New("dm: getting global lock id failed")
)

// NewDMLock helper function
func NewDMLock(in *DMBackend, l log.Logger, key, value string) (*DMLock, error) {
	// Create a new DM connection so we can close this and have no effect on
	// the rest of the DM backend and any cleanup that might need to be done.
	conn, _ := NewDMClient(in.conf, in.logger)

	dml := &DMLock{
		parentConn: in,
		in:         conn,
		logger:     l,
		statements: make(map[string]*sql.Stmt),
		key:        key,
		value:      value,
	}

	statements := map[string]string{
		"put": "INSERT INTO " + in.dbLockTable +
			" VALUES( ?, ? ) ON DUPLICATE KEY UPDATE current_leader=VALUES(current_leader)",
	}

	for name, query := range statements {
		if err := dml.prepare(name, query); err != nil {
			return nil, err
		}
	}

	return dml, nil
}

// prepare is a helper to prepare a query for future execution
func (dml *DMLock) prepare(name string, query string) error {
	stmt, err := dml.in.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare %q: %w", name, err)
	}
	dml.statements[name] = stmt
	return nil
}

// update the current cluster leader in the DB. This is used so
// we can tell the servers in standby who the active leader is.
func (dml *DMLock) becomeLeader() error {
	_, err := dml.statements["put"].Exec("leader", dml.value)
	if err != nil {
		return err
	}

	return nil
}

// Lock will try to get a lock for an indefinite amount of time
// based on the given key that has been requested.
func (dml *DMLock) Lock() error {
	defer metrics.MeasureSince([]string{"dm", "get_lock"}, time.Now())

	// Lock timeout math.MaxInt32 instead of -1 solves compatibility issues with
	// different DM flavours i.e. MariaDB
	rows, err := dml.in.Query("SELECT GET_LOCK(?, ?), IS_USED_LOCK(?)", dml.key, math.MaxInt32, dml.key)
	if err != nil {
		return err
	}

	defer rows.Close()
	rows.Next()
	var lock sql.NullInt64
	var connectionID sql.NullInt64
	rows.Scan(&lock, &connectionID)

	if rows.Err() != nil {
		return rows.Err()
	}

	// 1 is returned from GET_LOCK if it was able to get the lock
	// 0 if it failed and NULL if some strange error happened.
	if !lock.Valid || lock.Int64 != 1 {
		return ErrLockHeld
	}

	// Since we have the lock alert the rest of the cluster
	// that we are now the active leader.
	err = dml.becomeLeader()
	if err != nil {
		return ErrLockHeld
	}

	// This will return the connection ID of NULL if an error happens
	if !connectionID.Valid {
		return ErrSettingGlobalID
	}

	GlobalLockID = connectionID.Int64

	return nil
}

// Unlock just closes the connection. This is because closing the DM connection
// is a 100% reliable way to close the lock. If you just release the lock you must
// do it from the same dm connection_id that you originally created it from. This
// is a huge hastle and I actually couldn't find a clean way to do this although one
// likely does exist. Closing the connection however ensures we don't ever get into a
// state where we try to release the lock and it hangs it is also much less code.
func (dml *DMLock) Unlock() error {
	err := dml.in.Close()
	if err != nil {
		return ErrUnlockFailed
	}

	return nil
}

// Establish a TLS connection with a given CA certificate
// Register a tsl.Config associated with the same key as the dns param from sql.Open
// foo:bar@tcp(127.0.0.1:3306)/dbname?tls=default
func setupDMTLSConfig(tlsCaFile string) error {
	rootCertPool := x509.NewCertPool()

	pem, err := ioutil.ReadFile(tlsCaFile)
	if err != nil {
		return err
	}

	if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
		return err
	}

	// Because dm driver do not have this one, I have to give up
	// 	err = dm.RegisterTLSConfig(dmTLSKey, &tls.Config{
	// 		RootCAs: rootCertPool,
	// 	})
	// 	if err != nil {
	// 		return err
	// 	}

	return nil
}
