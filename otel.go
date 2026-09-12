package main

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

func init() {
	Tracer = otel.Tracer("raft")
}

var Tracer trace.Tracer

func InitTracer(ctx context.Context, addr, serviceName string, insecure bool) (*sdktrace.TracerProvider, error) {
	var err error
	var exporter *otlptrace.Exporter
	if insecure {
		exporter, err = otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(addr), otlptracehttp.WithInsecure())
	} else {
		exporter, err = otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(addr))
	}
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		)),
	)
	otel.SetTracerProvider(tp)
	Tracer = tp.Tracer(serviceName)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp, nil
}

func EndSpanWithError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
