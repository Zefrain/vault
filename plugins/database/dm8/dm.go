// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package plugdm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/go-secure-stdlib/strutil"
	dbplugin "github.com/hashicorp/vault/sdk/database/dbplugin/v5"
	"github.com/hashicorp/vault/sdk/database/helper/dbutil"
	"github.com/hashicorp/vault/sdk/helper/template"

	_ "dm"
)

const (
	defaultMysqlRevocationStmts = `
		REVOKE ALL PRIVILEGES, GRANT OPTION FROM '{{name}}'@'%';
		DROP USER '{{name}}'@'%'
	`

	defaultDMRotateCredentialsSQL = `
		ALTER USER '{{username}}'@'%' IDENTIFIED BY '{{password}}';
	`

	dmTypeName = "dm"

	DefaultUserNameTemplate       = `{{ printf "v-%s-%s-%s-%s" (.DisplayName | truncate 10) (.RoleName | truncate 10) (random 20) (unix_time) | truncate 32 }}`
	DefaultLegacyUserNameTemplate = `{{ printf "v-%s-%s-%s" (.RoleName | truncate 4) (random 20) | truncate 16 }}`
)

var _ dbplugin.Database = (*DM)(nil)

type DM struct {
	*dmSQLConnectionProducer

	usernameProducer        template.StringTemplate
	defaultUsernameTemplate string
}

// New implements builtinplugins.BuiltinFactory
func New(defaultUsernameTemplate string) func() (interface{}, error) {
	return func() (interface{}, error) {
		if defaultUsernameTemplate == "" {
			return nil, fmt.Errorf("missing default username template")
		}
		db := newDM(defaultUsernameTemplate)
		// Wrap the plugin with middleware to sanitize errors
		dbType := dbplugin.NewDatabaseErrorSanitizerMiddleware(db, db.SecretValues)

		return dbType, nil
	}
}

func newDM(defaultUsernameTemplate string) *DM {
	connProducer := &dmSQLConnectionProducer{}

	return &DM{
		dmSQLConnectionProducer: connProducer,
		defaultUsernameTemplate: defaultUsernameTemplate,
	}
}

func (d *DM) Type() (string, error) {
	return dmTypeName, nil
}

func (d *DM) getConnection(ctx context.Context) (*sql.DB, error) {
	db, err := d.Connection(ctx)
	if err != nil {
		return nil, err
	}

	return db.(*sql.DB), nil
}

func (d *DM) Initialize(ctx context.Context, req dbplugin.InitializeRequest) (dbplugin.InitializeResponse, error) {
	// Create a buffer to hold the stack trace
	// buf := make([]byte, 1<<16)

	// // Retrieve the stack trace
	// length := runtime.Stack(buf, true)

	// // Print the stack trace
	// fmt.Printf("Stack Trace:\n%s\n", buf[:length])

	usernameTemplate, err := strutil.GetString(req.Config, "username_template")
	if err != nil {
		return dbplugin.InitializeResponse{}, err
	}

	if usernameTemplate == "" {
		usernameTemplate = d.defaultUsernameTemplate
	}

	up, err := template.NewTemplate(template.Template(usernameTemplate))
	if err != nil {
		return dbplugin.InitializeResponse{}, fmt.Errorf("unable to initialize username template: %w", err)
	}

	d.usernameProducer = up

	_, err = d.usernameProducer.Generate(dbplugin.UsernameMetadata{})
	if err != nil {
		return dbplugin.InitializeResponse{}, fmt.Errorf("invalid username template: %w", err)
	}

	err = d.dmSQLConnectionProducer.Initialize(ctx, req.Config, req.VerifyConnection)
	if err != nil {
		return dbplugin.InitializeResponse{}, err
	}

	resp := dbplugin.InitializeResponse{
		Config: req.Config,
	}

	return resp, nil
}

func (d *DM) NewUser(ctx context.Context, req dbplugin.NewUserRequest) (dbplugin.NewUserResponse, error) {
	if len(req.Statements.Commands) == 0 {
		return dbplugin.NewUserResponse{}, dbutil.ErrEmptyCreationStatement
	}

	username, err := d.usernameProducer.Generate(req.UsernameConfig)
	if err != nil {
		return dbplugin.NewUserResponse{}, err
	}

	password := req.Password

	expirationStr := req.Expiration.Format("2006-01-02 15:04:05-0700")

	queryMap := map[string]string{
		"name":       username,
		"username":   username,
		"password":   password,
		"expiration": expirationStr,
	}

	if err := d.executePreparedStatementsWithMap(ctx, req.Statements.Commands, queryMap); err != nil {
		return dbplugin.NewUserResponse{}, err
	}

	resp := dbplugin.NewUserResponse{
		Username: username,
	}
	return resp, nil
}

func (d *DM) DeleteUser(ctx context.Context, req dbplugin.DeleteUserRequest) (dbplugin.DeleteUserResponse, error) {
	// Grab the read lock
	d.Lock()
	defer d.Unlock()

	// Get the connection
	db, err := d.getConnection(ctx)
	if err != nil {
		return dbplugin.DeleteUserResponse{}, err
	}

	revocationStmts := req.Statements.Commands
	// Use a default SQL statement for revocation if one cannot be fetched from the role
	if len(revocationStmts) == 0 {
		revocationStmts = []string{defaultMysqlRevocationStmts}
	}

	// Start a transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return dbplugin.DeleteUserResponse{}, err
	}
	defer tx.Rollback()

	for _, stmt := range revocationStmts {
		for _, query := range strutil.ParseArbitraryStringSlice(stmt, ";") {
			query = strings.TrimSpace(query)
			if len(query) == 0 {
				continue
			}

			// This is not a prepared statement because not all commands are supported
			// 1295: This command is not supported in the prepared statement protocol yet
			// Reference https://mariadb.com/kb/en/mariadb/prepare-statement/
			query = strings.ReplaceAll(query, "{{name}}", req.Username)
			query = strings.ReplaceAll(query, "{{username}}", req.Username)
			_, err = tx.ExecContext(ctx, query)
			if err != nil {
				return dbplugin.DeleteUserResponse{}, err
			}
		}
	}

	// Commit the transaction
	err = tx.Commit()
	return dbplugin.DeleteUserResponse{}, err
}

func (d *DM) UpdateUser(ctx context.Context, req dbplugin.UpdateUserRequest) (dbplugin.UpdateUserResponse, error) {
	if req.Password == nil && req.Expiration == nil {
		return dbplugin.UpdateUserResponse{}, fmt.Errorf("no change requested")
	}

	if req.Password != nil {
		err := d.changeUserPassword(ctx, req.Username, req.Password.NewPassword, req.Password.Statements.Commands)
		if err != nil {
			return dbplugin.UpdateUserResponse{}, fmt.Errorf("failed to change password: %w", err)
		}
	}

	// Expiration change/update is currently a no-op

	return dbplugin.UpdateUserResponse{}, nil
}

func (d *DM) changeUserPassword(ctx context.Context, username, password string, rotateStatements []string) error {
	if username == "" || password == "" {
		return errors.New("must provide both username and password")
	}

	if len(rotateStatements) == 0 {
		rotateStatements = []string{defaultDMRotateCredentialsSQL}
	}

	queryMap := map[string]string{
		"name":     username,
		"username": username,
		"password": password,
	}

	if err := d.executePreparedStatementsWithMap(ctx, rotateStatements, queryMap); err != nil {
		return err
	}
	return nil
}

// executePreparedStatementsWithMap loops through the given templated SQL statements and
// applies the map to them, interpolating values into the templates, returning
// the resulting username and password
func (d *DM) executePreparedStatementsWithMap(ctx context.Context, statements []string, queryMap map[string]string) error {
	// Grab the lock
	d.Lock()
	defer d.Unlock()

	// Get the connection
	db, err := d.getConnection(ctx)
	if err != nil {
		return err
	}
	// Start a transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Execute each query
	for _, stmt := range statements {
		for _, query := range strutil.ParseArbitraryStringSlice(stmt, ";") {
			query = strings.TrimSpace(query)
			if len(query) == 0 {
				continue
			}

			query = dbutil.QueryHelper(query, queryMap)

			stmt, err := tx.PrepareContext(ctx, query)
			if err != nil {
				// If the error code we get back is Error 1295: This command is not
				// supported in the prepared statement protocol yet, we will execute
				// the statement without preparing it. This allows the caller to
				// manually prepare statements, as well as run other not yet
				// prepare supported commands. If there is no error when running we
				// will continue to the next statement.
				// TODO: Error?

				// if e, ok := err.(*stddm.MySQLError); ok && e.Number == 1295 {
				// 	_, err = tx.ExecContext(ctx, query)
				// 	if err != nil {
				// 		stmt.Close()
				// 		return err
				// 	}
				// 	continue
				// }

				return err
			}
			if _, err := stmt.ExecContext(ctx); err != nil {
				stmt.Close()
				return err
			}
			stmt.Close()
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
