package gormstart

import (
	"github.com/google/wire"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"go.didapinche.com/agollo/v2"
	"go.didapinche.com/boot"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"time"
)

// Options is  configuration of database
type Options struct {
	URL         string        `yaml:"url" properties:"url"`
	Debug       bool          `yaml:"debug" properties:"debug"`
	MaxIdle     int           `yaml:"maxIdle" properties:"maxIdle"`
	MaxOpen     int           `yaml:"maxOpen" properties:"maxOpen"`
	MaxLifetime time.Duration `yaml:"maxLifeTime" properties:"maxLifeTime"` // maximum amount of time a connection may be reused
	MaxIdleTime time.Duration `yaml:"maxIdleTime" properties:"maxIdleTime"` // maximum amount of time a connection may be idle before being closed
	boot.Option
}

// NewOptions build database option from viper
func NewOptions() (*Options, error) {
	var err error
	o := new(Options)
	if err := boot.AddOption("datasource", o); err != nil {
		return nil, errors.Wrapf(err, "add grom option to boot error")
	}

	return o, err
}

// New returns *gorm.DB that used for mysql operate
func New(o *Options, logger *zap.Logger, tracer opentracing.Tracer) (*gorm.DB, error) {
	var db *gorm.DB

	fn := func() error {
		idb, err := gorm.Open(mysql.Open(o.URL), &gorm.Config{})
		if err != nil {
			return errors.Wrap(err, "gorm open database connection error")
		}

		sqlDB, err := idb.DB()
		if err != nil {
			return errors.Wrapf(err, "get sql db error")
		}

		sqlDB.SetMaxIdleConns(o.MaxIdle)
		sqlDB.SetMaxOpenConns(o.MaxOpen)
		sqlDB.SetConnMaxIdleTime(o.MaxIdleTime)
		sqlDB.SetConnMaxLifetime(o.MaxLifetime)

		if o.Debug {
			idb = idb.Debug()
		}

		before := func(db *gorm.DB) {
			//ctx := db.Statement.Context
			//span := tracer.StartSpan("sql", opentracing.ChildOf(opentracing.SpanFromContext(ctx).Context()))
		}

		after := func(db *gorm.DB) {

		}

		if err := db.Callback().Create().Before("gorm:create").Register("trace:before", before); err != nil {
			return errors.Wrap(err, "grom create callback register tracer:before error")
		}
		if err := db.Callback().Create().After("gorm:create").Register("trace:after", after); err != nil {
			return errors.Wrap(err, "grom create callback register tracer:after error")
		}
		if err := db.Callback().Query().Before("gorm:query").Register("trace:before", before); err != nil {
			return errors.Wrap(err, "grom query callback register tracer:before error")
		}
		if err := db.Callback().Query().After("gorm:query").Register("trace:after", after); err != nil {
			return errors.Wrap(err, "grom query callback register tracer:after error")
		}
		if err := db.Callback().Delete().Before("gorm:delete").Register("trace:before", before); err != nil {
			return errors.Wrap(err, "grom delete callback register tracer:before error")
		}
		if err := db.Callback().Delete().After("gorm:delete").Register("trace:after", after); err != nil {
			return errors.Wrap(err, "grom delete callback register tracer:after error")
		}
		if err := db.Callback().Update().Before("gorm:update").Register("trace:before", before); err != nil {
			return errors.Wrap(err, "grom update callback register tracer:before error")
		}
		if err := db.Callback().Update().After("gorm:update").Register("trace:after", after); err != nil {
			return errors.Wrap(err, "grom update callback register tracer:after error")
		}

		if db != nil {
			*db = *idb
		} else {
			db = idb
		}

		logger.Info("initialize database success")
		return nil
	}

	o.Change = func(event *agollo.ChangeEvent) error {

		if err := fn(); err != nil {
			return errors.Wrapf(err, "refresh grom DB error")
		}
		return nil
	}

	if err := fn(); err != nil {
		return nil, errors.Wrap(err, "initialize database error")
	}

	return db, nil
}

// ProviderSet define provider set of db
var ProviderSet = wire.NewSet(New, NewOptions)
