package logic

import (
	"context"
	"extraction-pipeline/internal/db"
	"fmt"
	"log/slog"
	"math"
)

type ProductivityProcessor struct {
	sqlite       ProductivityRepository
	breaksConfig map[string]map[string]float64
}

func NewProductivityProcessor(sqlite ProductivityRepository, breaksConfig map[string]map[string]float64) *ProductivityProcessor {
	return &ProductivityProcessor{
		sqlite:       sqlite,
		breaksConfig: breaksConfig,
	}
}

type bucketKey struct {
	LGNUM, Flow, Floor, Hour string
}

type userHourKey struct {
	Operator, Hour string
}

func round(val float64) float64 {
	return math.Round(val*100) / 100
}

func (p *ProductivityProcessor) CalculateHourlyProductivity(ctx context.Context, date string) error {
	slog.Info("calculating sql-offloaded picking productivity", "date", date)

	// 1. Fetch Aggregated Data from SQLite
	aggs, err := p.sqlite.GetHourlyPickingAggregation(ctx, date)
	if err != nil {
		return fmt.Errorf("failed to fetch picking aggregation: %w", err)
	}

	if len(aggs) == 0 {
		slog.Warn("no picking records found for productivity calculation", "date", date)
		return nil
	}

	// 2. Track unique buckets per operator-hour for time splitting
	userHourBuckets := make(map[userHourKey]int)
	cohortTotals := make(map[bucketKey]*aggStats)

	for _, a := range aggs {
		uk := userHourKey{Operator: a.Operator, Hour: a.Hour}
		userHourBuckets[uk]++

		bk := bucketKey{LGNUM: a.LGNUM, Flow: a.Flow, Floor: a.Floor, Hour: a.Hour}
		if cohortTotals[bk] == nil {
			cohortTotals[bk] = &aggStats{}
		}
		cohortTotals[bk].LineCount += a.LineCount
		cohortTotals[bk].ItemQuantity += a.ItemQuantity
		cohortTotals[bk].TotalWeight += a.TotalWeight
	}

	// 3. Process and Calculate
	var finalRecords []db.HourlyProductivityRecord
	for _, a := range aggs {
		uk := userHourKey{Operator: a.Operator, Hour: a.Hour}
		bk := bucketKey{LGNUM: a.LGNUM, Flow: a.Flow, Floor: a.Floor, Hour: a.Hour}
		
		// Effective Hour Calculation
		breakTime := 0.0
		if lgMap, ok := p.breaksConfig[a.LGNUM]; ok {
			if b, ok := lgMap[a.Hour]; ok {
				breakTime = b
			}
		}
		
		totalAvail := 1.0 - breakTime
		if totalAvail < 0 { totalAvail = 0 }
		eh := totalAvail / float64(userHourBuckets[uk])

		// Productivity
		prod := 0.0
		if eh > 0 {
			prod = float64(a.LineCount) / eh
		}

		// Intensity
		cohort := cohortTotals[bk]
		avgWeightLine := cohort.TotalWeight / float64(cohort.LineCount)
		avgItemLine := cohort.ItemQuantity / float64(cohort.LineCount)

		userWeightLine := a.TotalWeight / float64(a.LineCount)
		userItemLine := a.ItemQuantity / float64(a.LineCount)

		wInt := 1.0
		if avgWeightLine > 0 { wInt = userWeightLine / avgWeightLine }
		iInt := 1.0
		if avgItemLine > 0 { iInt = userItemLine / avgItemLine }

		adjProd := prod * ((wInt + iInt) / 2.0)

		finalRecords = append(finalRecords, db.HourlyProductivityRecord{
			Date: date, LGNUM: a.LGNUM, Flow: a.Flow, Floor: a.Floor, Hour: a.Hour, Operator: a.Operator,
			LineCount: a.LineCount, ItemQuantity: round(a.ItemQuantity), TotalWeight: round(a.TotalWeight),
			TotalVolumeM3: round(a.TotalVolumeM3), EffectiveHours: eh, BaseProductivity: round(prod),
			WeightIntensity: round(wInt), ItemIntensity: round(iInt), AdjustedProductivity: round(adjProd),
		})
	}

	return p.sqlite.InsertProductivity(ctx, date, finalRecords)
}

func (p *ProductivityProcessor) CalculateDailyProductivity(ctx context.Context, date string) error {
	slog.Info("calculating daily picking productivity", "date", date)
	hourlyRecords, err := p.sqlite.GetHourlyProductivityRecords(ctx, date)
	if err != nil {
		return err
	}
	if len(hourlyRecords) == 0 {
		return nil
	}

	userDailyData := make(map[dailyKey]*aggStatsDaily)
	for _, hr := range hourlyRecords {
		dk := dailyKey{LGNUM: hr.LGNUM, Flow: hr.Flow, Floor: hr.Floor, Operator: hr.Operator}
		if userDailyData[dk] == nil {
			userDailyData[dk] = &aggStatsDaily{}
		}
		userDailyData[dk].LineCount += hr.LineCount
		userDailyData[dk].ItemQuantity += hr.ItemQuantity
		userDailyData[dk].TotalWeight += hr.TotalWeight
		userDailyData[dk].TotalVolume += hr.TotalVolumeM3
		userDailyData[dk].EffectiveHours += hr.EffectiveHours
	}

	cohortDailyTotals := make(map[cohortKey]*aggStatsDaily)
	for dk, stats := range userDailyData {
		ck := cohortKey{LGNUM: dk.LGNUM, Flow: dk.Flow, Floor: dk.Floor}
		if cohortDailyTotals[ck] == nil {
			cohortDailyTotals[ck] = &aggStatsDaily{}
		}
		cohortDailyTotals[ck].LineCount += stats.LineCount
		cohortDailyTotals[ck].ItemQuantity += stats.ItemQuantity
		cohortDailyTotals[ck].TotalWeight += stats.TotalWeight
		cohortDailyTotals[ck].EffectiveHours += stats.EffectiveHours
	}

	var dailyRecords []db.DailyProductivityRecord
	for dk, stats := range userDailyData {
		ck := cohortKey{LGNUM: dk.LGNUM, Flow: dk.Flow, Floor: dk.Floor}
		cohort := cohortDailyTotals[ck]

		prod := 0.0
		if stats.EffectiveHours > 0 { prod = float64(stats.LineCount) / stats.EffectiveHours }

		avgW := cohort.TotalWeight / float64(cohort.LineCount)
		avgI := cohort.ItemQuantity / float64(cohort.LineCount)
		userW := stats.TotalWeight / float64(stats.LineCount)
		userI := stats.ItemQuantity / float64(stats.LineCount)

		wInt := 1.0
		if avgW > 0 { wInt = userW / avgW }
		iInt := 1.0
		if avgI > 0 { iInt = userI / avgI }

		dailyRecords = append(dailyRecords, db.DailyProductivityRecord{
			Date: date, LGNUM: dk.LGNUM, Flow: dk.Flow, Floor: dk.Floor, Operator: dk.Operator,
			LineCount: stats.LineCount, ItemQuantity: round(stats.ItemQuantity), TotalWeight: round(stats.TotalWeight),
			TotalVolumeM3: round(stats.TotalVolume), TotalHours: round(stats.EffectiveHours),
			BaseProductivity: round(prod), WeightIntensity: round(wInt), ItemIntensity: round(iInt), AdjustedProductivity: round(prod * (wInt + iInt) / 2.0),
		})
	}
	return p.sqlite.InsertDailyProductivity(ctx, date, dailyRecords)
}

func (p *ProductivityProcessor) CalculateHourlyPackingProductivity(ctx context.Context, date string) error {
	slog.Info("calculating sql-offloaded packing productivity", "date", date)
	aggs, err := p.sqlite.GetHourlyPackingAggregation(ctx, date)
	if err != nil {
		return err
	}
	if len(aggs) == 0 {
		return nil
	}

	userHourBuckets := make(map[userHourKey]int)
	for _, a := range aggs {
		uk := userHourKey{Operator: a.Operator, Hour: a.Hour}
		userHourBuckets[uk]++
	}

	var hourlyRecords []db.HourlyPackingRecord
	for _, a := range aggs {
		uk := userHourKey{Operator: a.Operator, Hour: a.Hour}
		
		breakTime := 0.0
		if lgMap, ok := p.breaksConfig[a.LGNUM]; ok {
			if b, ok := lgMap[a.Hour]; ok {
				breakTime = b
			}
		}
		
		totalAvail := 1.0 - breakTime
		if totalAvail < 0 { totalAvail = 0 }
		eh := totalAvail / float64(userHourBuckets[uk])

		prod := 0.0
		if eh > 0 { prod = float64(a.BoxCount) / eh }

		hourlyRecords = append(hourlyRecords, db.HourlyPackingRecord{
			Date: date, LGNUM: a.LGNUM, Hour: a.Hour, Operator: a.Operator, Flow: a.Flow, Floor: a.Floor,
			BoxCount: a.BoxCount, EffectiveHours: eh, Productivity: round(prod),
		})
	}
	return p.sqlite.InsertPackingProductivity(ctx, date, hourlyRecords)
}

func (p *ProductivityProcessor) CalculateDailyPackingProductivity(ctx context.Context, date string) error {
	slog.Info("calculating daily packing productivity", "date", date)
	
	aggs, _ := p.sqlite.GetHourlyPackingAggregation(ctx, date)
	hourly, _ := p.sqlite.GetHourlyPackingProductivityRecords(ctx, date)

	if len(aggs) == 0 { return nil }

	type dailyPkKey struct { LGNUM, Operator, Flow, Floor string }
	dailyBoxes := make(map[dailyPkKey]int)
	for _, a := range aggs {
		dk := dailyPkKey{LGNUM: a.LGNUM, Operator: a.Operator, Flow: a.Flow, Floor: a.Floor}
		dailyBoxes[dk] += a.BoxCount
	}

	var dailyRecords []db.DailyPackingRecord
	for dk, count := range dailyBoxes {
		totalEh := 0.0
		for _, hr := range hourly {
			if hr.LGNUM == dk.LGNUM && hr.Operator == dk.Operator && hr.Flow == dk.Flow && hr.Floor == dk.Floor {
				totalEh += hr.EffectiveHours
			}
		}

		prod := 0.0
		if totalEh > 0 { prod = float64(count) / totalEh }

		dailyRecords = append(dailyRecords, db.DailyPackingRecord{
			Date: date, LGNUM: dk.LGNUM, Operator: dk.Operator, Flow: dk.Flow, Floor: dk.Floor,
			BoxCount: count, Productivity: round(prod),
		})
	}
	return p.sqlite.InsertDailyPackingProductivity(ctx, date, dailyRecords)
}

type aggStats struct {
	LineCount    int
	ItemQuantity float64
	TotalWeight  float64
}

type dailyKey struct {
	LGNUM, Flow, Floor, Operator string
}

type cohortKey struct {
	LGNUM, Flow, Floor string
}

type aggStatsDaily struct {
	LineCount      int
	ItemQuantity  float64
	TotalWeight    float64
	TotalVolume    float64
	EffectiveHours float64
}



