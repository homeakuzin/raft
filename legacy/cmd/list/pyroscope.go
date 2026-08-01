package main

import (
	"fmt"
	"log/slog"
	"os"
	"runtime"

	"github.com/grafana/pyroscope-go"
)

func startPyroscope(logger *slog.Logger, nodeID string) func() {
	addr := os.Getenv("PYROSCOPE_SERVER_ADDRESS")
	if addr == "" {
		return func() {}
	}

	appName := os.Getenv("PYROSCOPE_APPLICATION_NAME")
	if appName == "" {
		appName = "raft-list"
	}

	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)

	profiler, err := pyroscope.Start(pyroscope.Config{
		ApplicationName:   appName,
		ServerAddress:     addr,
		BasicAuthUser:     os.Getenv("PYROSCOPE_BASIC_AUTH_USER"),
		BasicAuthPassword: os.Getenv("PYROSCOPE_BASIC_AUTH_PASSWORD"),
		TenantID:          os.Getenv("PYROSCOPE_TENANT_ID"),
		Logger:            slogPyroscopeLogger{logger: logger},
		Tags: map[string]string{
			"env":      os.Getenv("APP_ENV"),
			"hostname": os.Getenv("HOSTNAME"),
			"node_id":  nodeID,
			"version":  os.Getenv("APP_VERSION"),
		},
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount,
			pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount,
			pyroscope.ProfileBlockDuration,
		},
	})
	if err != nil {
		logger.Error("could not start pyroscope", "err", err)
		return func() {}
	}

	logger.Info("exporting pyroscope profiles", "addr", addr, "app", appName)
	return func() {
		if err := profiler.Stop(); err != nil {
			logger.Error("could not stop pyroscope", "err", err)
		}
	}
}

type slogPyroscopeLogger struct {
	logger *slog.Logger
}

func (l slogPyroscopeLogger) Infof(format string, args ...interface{}) {
	l.logger.Info("pyroscope: " + fmt.Sprintf(format, args...))
}

func (l slogPyroscopeLogger) Debugf(format string, args ...interface{}) {
	l.logger.Debug("pyroscope: " + fmt.Sprintf(format, args...))
}

func (l slogPyroscopeLogger) Errorf(format string, args ...interface{}) {
	l.logger.Error("pyroscope: " + fmt.Sprintf(format, args...))
}
