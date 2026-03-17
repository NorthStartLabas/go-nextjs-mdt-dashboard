package logic

import (
	"extraction-pipeline/internal/db"
	"fmt"
	"math"
)

type ProductivityProcessor struct {
	sqlite       *db.SQLiteClient
	breaksConfig map[string]map[string]float64
}

func NewProductivityProcessor(sqlite *db.SQLiteClient, breaksConfig map[string]map[string]float64) *ProductivityProcessor {
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

type aggStats struct {
	LineCount    int
	ItemQuantity float64
	TotalWeight  float64
	TotalVolume  float64
}

func round(val float64) float64 {
	return math.Round(val*100) / 100
}

func (p *ProductivityProcessor) CalculateHourlyProductivity(date string) error {
	fmt.Printf("Calculating productivity for %s...\n", date)

	// 1. Fetch all raw picking data for the date
	records, err := p.sqlite.GetRawPickingRecords(date)
	if err != nil {
		return fmt.Errorf("failed to fetch raw records: %w", err)
	}

	if len(records) == 0 {
		fmt.Println("No records found for productivity calculation.")
		return nil
	}

	// 2. Aggregate raw data into buckets
	// userHourBuckets tracks unique (LGNUM, Flow, Floor) combinations per operator-hour
	userHourBuckets := make(map[userHourKey]map[bucketKey]bool)
	// hourlyData stores the actual sums for each (Bucket + Operator)
	type fullKey struct {
		bucketKey
		Operator string
	}
	hourlyData := make(map[fullKey]*aggStats)

	for _, r := range records {
		// Extract Hour (format HH)
		hourStr := "00"
		if len(r.QZEIT) >= 2 {
			hourStr = r.QZEIT[0:2]
		}

		bk := bucketKey{LGNUM: r.LGNUM, Flow: r.FLOW, Floor: r.FLOOR, Hour: hourStr}
		uk := userHourKey{Operator: r.OPERATOR, Hour: hourStr}
		fk := fullKey{bucketKey: bk, Operator: r.OPERATOR}

		// Track unique buckets per user-hour for time splitting
		if userHourBuckets[uk] == nil {
			userHourBuckets[uk] = make(map[bucketKey]bool)
		}
		userHourBuckets[uk][bk] = true

		// Accumulate stats
		if hourlyData[fk] == nil {
			hourlyData[fk] = &aggStats{}
		}
		hourlyData[fk].LineCount++
		hourlyData[fk].ItemQuantity += r.NISTA
		hourlyData[fk].TotalWeight += r.BRGEW
		hourlyData[fk].TotalVolume += (r.VOLUM / 1000000.0) // ccm to m3
	}

	// 3. Calculate Effective Hours per Bucket-Operator
	// Pre-calculate effective hours per fullKey
	effectiveHoursMap := make(map[fullKey]float64)
	for uk, buckets := range userHourBuckets {
		// Base available time
		baseTime := 1.0
		if lgMap, ok := p.breaksConfig["245"]; ok { // Default to 245 for global check or check per bucket in loop
			// Note: We'll use the specific LGNUM logic inside the next loop
			_ = lgMap
		}

		bucketCount := float64(len(buckets))
		for bk := range buckets {
			// Find break for this specific LGNUM and Hour
			breakDuration := 0.0
			if lgMap, ok := p.breaksConfig[bk.LGNUM]; ok {
				if b, ok := lgMap[bk.Hour]; ok {
					breakDuration = b
				}
			}
			
			totalAvailable := baseTime - breakDuration
			if totalAvailable < 0 { totalAvailable = 0 }
			
			fk := fullKey{bucketKey: bk, Operator: uk.Operator}
			effectiveHoursMap[fk] = totalAvailable / bucketCount
		}
	}

	// 4. Calculate Cohort Baselines (Aggregates for all operators in a bucket)
	cohortTotals := make(map[bucketKey]*aggStats)
	for fk, stats := range hourlyData {
		if cohortTotals[fk.bucketKey] == nil {
			cohortTotals[fk.bucketKey] = &aggStats{}
		}
		cohortTotals[fk.bucketKey].LineCount += stats.LineCount
		cohortTotals[fk.bucketKey].ItemQuantity += stats.ItemQuantity
		cohortTotals[fk.bucketKey].TotalWeight += stats.TotalWeight
		cohortTotals[fk.bucketKey].TotalVolume += stats.TotalVolume
	}

	// 5. Build final records for insertion
	var finalRecords []db.HourlyProductivityRecord
	for fk, stats := range hourlyData {
		eh := effectiveHoursMap[fk]
		
		prod := 0.0
		if eh > 0 {
			prod = float64(stats.LineCount) / eh
		}

		// Calculate Intensities
		// Global Average per Line for this bucket
		cohort := cohortTotals[fk.bucketKey]
		avgWeightLine := cohort.TotalWeight / float64(cohort.LineCount)
		avgItemLine := cohort.ItemQuantity / float64(cohort.LineCount)

		// User Average per Line
		userWeightLine := stats.TotalWeight / float64(stats.LineCount)
		userItemLine := stats.ItemQuantity / float64(stats.LineCount)

		wIntensity := 1.0
		if avgWeightLine > 0 {
			wIntensity = userWeightLine / avgWeightLine
		}

		iIntensity := 1.0
		if avgItemLine > 0 {
			iIntensity = userItemLine / avgItemLine
		}

		// Combined Multiplier
		combinedMultiplier := (wIntensity + iIntensity) / 2.0
		adjProd := prod * combinedMultiplier

		finalRecords = append(finalRecords, db.HourlyProductivityRecord{
			Date:                 date,
			LGNUM:                fk.LGNUM,
			Flow:                 fk.Flow,
			Floor:                fk.Floor,
			Hour:                 fk.Hour,
			Operator:             fk.Operator,
			LineCount:            stats.LineCount,
			ItemQuantity:         round(stats.ItemQuantity),
			TotalWeight:          round(stats.TotalWeight),
			TotalVolumeM3:        round(stats.TotalVolume),
			EffectiveHours:       eh,
			BaseProductivity:     round(prod),
			WeightIntensity:      round(wIntensity),
			ItemIntensity:        round(iIntensity),
			AdjustedProductivity: round(adjProd),
		})
	}

	// 6. Save results
	if err := p.sqlite.InsertProductivity(date, finalRecords); err != nil {
		return fmt.Errorf("failed to save productivity: %w", err)
	}

	fmt.Printf("Successfully calculated and saved %d productivity rows.\n", len(finalRecords))
	return nil
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

func (p *ProductivityProcessor) CalculateDailyProductivity(date string) error {
	fmt.Printf("Calculating daily productivity for %s...\n", date)

	// 1. Fetch hourly records
	hourlyRecords, err := p.sqlite.GetHourlyProductivityRecords(date)
	if err != nil {
		return fmt.Errorf("failed to fetch hourly records: %w", err)
	}

	if len(hourlyRecords) == 0 {
		fmt.Println("No hourly records found for daily aggregation.")
		return nil
	}

	// 2. Aggregate metrics per operator grouping
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

	// 3. Calculate Cohort Baselines for the day
	cohortDailyTotals := make(map[cohortKey]*aggStatsDaily)
	for dk, stats := range userDailyData {
		ck := cohortKey{LGNUM: dk.LGNUM, Flow: dk.Flow, Floor: dk.Floor}
		if cohortDailyTotals[ck] == nil {
			cohortDailyTotals[ck] = &aggStatsDaily{}
		}
		cohortDailyTotals[ck].LineCount += stats.LineCount
		cohortDailyTotals[ck].ItemQuantity += stats.ItemQuantity
		cohortDailyTotals[ck].TotalWeight += stats.TotalWeight
		cohortDailyTotals[ck].TotalVolume += stats.TotalVolume
		cohortDailyTotals[ck].EffectiveHours += stats.EffectiveHours
	}

	// 4. Build final daily records
	var dailyRecords []db.DailyProductivityRecord
	for dk, stats := range userDailyData {
		ck := cohortKey{LGNUM: dk.LGNUM, Flow: dk.Flow, Floor: dk.Floor}
		cohort := cohortDailyTotals[ck]

		// Base Productivity
		prod := 0.0
		if stats.EffectiveHours > 0 {
			prod = float64(stats.LineCount) / stats.EffectiveHours
		}

		// Intensity Recalculation (Daily Cohort Average)
		avgWeightLine := cohort.TotalWeight / float64(cohort.LineCount)
		avgItemLine := cohort.ItemQuantity / float64(cohort.LineCount)

		userWeightLine := stats.TotalWeight / float64(stats.LineCount)
		userItemLine := stats.ItemQuantity / float64(stats.LineCount)

		wIntensity := 1.0
		if avgWeightLine > 0 {
			wIntensity = userWeightLine / avgWeightLine
		}

		iIntensity := 1.0
		if avgItemLine > 0 {
			iIntensity = userItemLine / avgItemLine
		}

		combinedMultiplier := (wIntensity + iIntensity) / 2.0
		adjProd := prod * combinedMultiplier

		dailyRecords = append(dailyRecords, db.DailyProductivityRecord{
			Date:                 date,
			LGNUM:                dk.LGNUM,
			Flow:                 dk.Flow,
			Floor:                dk.Floor,
			Operator:             dk.Operator,
			LineCount:            stats.LineCount,
			ItemQuantity:         round(stats.ItemQuantity),
			TotalWeight:          round(stats.TotalWeight),
			TotalVolumeM3:        round(stats.TotalVolume),
			TotalHours:           round(stats.EffectiveHours),
			BaseProductivity:     round(prod),
			WeightIntensity:      round(wIntensity),
			ItemIntensity:        round(iIntensity),
			AdjustedProductivity: round(adjProd),
		})
	}

	// 5. Save results
	if err := p.sqlite.InsertDailyProductivity(date, dailyRecords); err != nil {
		return fmt.Errorf("failed to save daily productivity: %w", err)
	}

	fmt.Printf("Successfully calculated and saved %d daily productivity rows.\n", len(dailyRecords))
	return nil
}

