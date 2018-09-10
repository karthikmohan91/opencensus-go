// Copyright 2017, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package prometheus contains a Prometheus exporter that supports exporting
// OpenCensus views as Prometheus metrics.
package influxdb // import "go.opencensus.io/exporter/influxdb"

import (
	"fmt"
	"strconv"

	influxdb "github.com/influxdata/influxdb/client/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

type Exporter struct {
	c                influxdb.Client
	database         string
	rp               string
	precision        string
	writeConsistency string
	tags             map[string]string
	onError          func(err error)
}

type Options struct {
	Database           string
	RetentionPolicy    string
	InfluxClientConfig influxdb.HTTPConfig
	Precision          string
	WriteConsistency   string
	Tags               map[string]string
	OnError            func(err error)
}

// NewExporter returns an exporter that exports stats to InfluxDB.
func NewExporter(o Options) (*Exporter, error) {
	e := &Exporter{}
	c, err := influxdb.NewHTTPClient(o.InfluxClientConfig)
	if err != nil {
		return nil, err
	}
	e.c = c
	e.rp = o.RetentionPolicy
	e.database = o.Database
	e.writeConsistency = o.WriteConsistency
	e.precision = o.Precision
	e.onError = o.OnError
	e.tags = o.Tags
	return e, nil
}

func toPoint(r *view.Row, v *view.View, tags map[string]string) ([]*influxdb.Point, error) {
	fields := map[string]interface{}{}

	for _, t := range r.Tags {
		tags[t.Key.Name()] = t.Value
	}

	if v.Measure.Unit() != stats.UnitDimensionless {
		fields["unit"] = v.Measure.Unit()
	}

	points := []*influxdb.Point{}
	switch data := r.Data.(type) {
	case *view.CountData:
		fields["counter"] = float64(data.Value)
		point, err := influxdb.NewPoint(v.Name, tags, fields)
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	case *view.DistributionData:
		fields["sum"] = data.Sum()
		fields["count"] = data.Count
		for i, bucket := range v.Aggregation.Buckets {
			fields[strconv.FormatFloat(bucket, 'f', -1, 64)] = data.CountPerBucket[i]
		}
		fields["+Inf"] = data.CountPerBucket[len(data.CountPerBucket)-1]

		point, err := influxdb.NewPoint(v.Name, tags, fields)
		if err != nil {
			return nil, err
		}
		points = append(points, point)

	case *view.SumData:
		fields["sum"] = data.Value
		point, err := influxdb.NewPoint(v.Name, tags, fields)
		if err != nil {
			return nil, err
		}
		points = append(points, point)

	case *view.LastValueData:
		fields["last"] = data.Value
		point, err := influxdb.NewPoint(v.Name, tags, fields)
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	default:
		return nil, fmt.Errorf("aggregation %T is not yet supported", v.Aggregation)
	}

	return points, nil
}

func (e *Exporter) ExportView(viewData *view.Data) {
	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Precision:        e.precision,
		Database:         e.database,
		RetentionPolicy:  e.rp,
		WriteConsistency: e.writeConsistency,
	})
	if err != nil {
		e.onError(err)
		return
	}
	for ii := 0; ii < len(viewData.Rows); ii++ {
		points, err := toPoint(viewData.Rows[ii], viewData.View, e.tags)
		if err != nil {
			e.onError(err)
		}
		bp.AddPoints(points)
	}
	err = e.c.Write(bp)
	if err != nil {
		e.onError(err)
	}
}
