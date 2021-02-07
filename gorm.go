package gormstart

import (
	"github.com/google/wire"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql" // mysql driver
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"time"
)

// Options is  configuration of database
type Options struct {
	URL         string `yaml:"url"`
	Debug       bool
	MaxIdle     int
	MaxOpen     int
	MaxLifetime time.Duration // maximum amount of time a connection may be reused
	MaxIdleTime time.Duration // maximum amount of time a connection may be idle before being closed

}

// NewOptions build database option from viper
func NewOptions(v *viper.Viper) (*Options, error) {
	var err error
	o := new(Options)
	if err = v.UnmarshalKey("gorm", o); err != nil {
		return nil, errors.Wrap(err, "unmarshal db option error")
	}

	return o, err
}

// New returns *gorm.DB that used for mysql operate
func New(o *Options, logger *zap.Logger) (*gorm.DB, error) {
	var err error
	db, err := gorm.Open("mysql", o.URL)
	if err != nil {
		return nil, errors.Wrap(err, "gorm open database connection error")
	}

	db.DB().SetMaxIdleConns(o.MaxIdle)
	db.DB().SetMaxOpenConns(o.MaxOpen)
	db.DB().SetConnMaxIdleTime(o.MaxIdleTime)
	db.DB().SetConnMaxLifetime(o.MaxLifetime)

	if o.Debug {
		db = db.Debug()
	}

	logger.Info("initialize database success", zap.String("url", o.URL))

	return db, nil
}

// ProviderSet define provider set of db
var ProviderSet = wire.NewSet(New, NewOptions)
