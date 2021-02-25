package gormstart

import (
	"github.com/google/wire"
	"github.com/opentracing/opentracing-go"
	tracerLog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"go.didapinche.com/agollo/v2"
	"go.didapinche.com/boot"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"strings"
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
			// 先从父级spans生成子span ---> 这里命名为gorm，但实际上可以自定义
			// 自己喜欢的operationName

			opentracing.SetGlobalTracer(tracer)
			span, _ := opentracing.StartSpanFromContext(db.Statement.Context, "gorm")

			// 利用db实例去传递span
			db.InstanceSet("__gorm_span", span)

			return
			//ctx := db.Statement.Context
			//span := tracer.StartSpan("sql", opentracing.ChildOf(opentracing.SpanFromContext(ctx).Context()))
		}

		after := func(db *gorm.DB) {
			// 从GORM的DB实例中取出span
			_span, isExist := db.InstanceGet("__gorm_span")
			if !isExist {
				// 不存在就直接抛弃掉
				return
			}
			// 断言进行类型转换
			span, ok := _span.(opentracing.Span)
			if !ok {
				return
			}
			operate := strings.Split(db.Statement.SQL.String(), " ")

			span.SetOperationName(operate[0])
			span.SetTag("sql", db.Statement.SQL.String())
			defer span.Finish()
			if db.Error != nil {
				span.LogFields(tracerLog.Error(db.Error))
			}
			//span.LogFields(tracerLog.String("sql", db.Dialector.Explain(db.Statement.SQL.String(), db.Statement.Vars...)))
			return
		}

		if err := idb.Callback().Create().Before("gorm:create").Register("trace:before", before); err != nil {
			return errors.Wrap(err, "grom create callback register tracer:before error")
		}
		if err := idb.Callback().Create().After("gorm:create").Register("trace:after", after); err != nil {
			return errors.Wrap(err, "grom create callback register tracer:after error")
		}
		if err := idb.Callback().Query().Before("gorm:query").Register("trace:before", before); err != nil {
			return errors.Wrap(err, "grom query callback register tracer:before error")
		}
		if err := idb.Callback().Query().After("gorm:query").Register("trace:after", after); err != nil {
			return errors.Wrap(err, "grom query callback register tracer:after error")
		}
		if err := idb.Callback().Delete().Before("gorm:delete").Register("trace:before", before); err != nil {
			return errors.Wrap(err, "grom delete callback register tracer:before error")
		}
		if err := idb.Callback().Delete().After("gorm:delete").Register("trace:after", after); err != nil {
			return errors.Wrap(err, "grom delete callback register tracer:after error")
		}
		if err := idb.Callback().Update().Before("gorm:update").Register("trace:before", before); err != nil {
			return errors.Wrap(err, "grom update callback register tracer:before error")
		}
		if err := idb.Callback().Update().After("gorm:update").Register("trace:after", after); err != nil {
			return errors.Wrap(err, "grom update callback register tracer:after error")
		}
		if err := idb.Callback().Raw().Before("gorm:raw").Register("trace:before", before); err != nil {
			return errors.Wrap(err, "grom raw callback register tracer:before error")
		}
		if err := idb.Callback().Raw().After("gorm:raw").Register("trace:after", after); err != nil {
			return errors.Wrap(err, "grom raw callback register tracer:after error")
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
