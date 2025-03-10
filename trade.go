package main

import (
	"encoding/csv"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	TIGER_TRADE_DATETIME_FORMAT = "2006/01/02 15:04:05"
)

type Trade struct {
	FinancialInstrument string
	Market              string
	Name                string
	Side                string
	Quantity            int
	Price               float64
	TotalAmount         float64
	TotalCharge         float64
	GST                 float64
	SECFee              float64
	OptionRegulatoryFee float64
	ClearingFee         float64
	TradingActivityFee  float64
	Commission          float64
	PlatformFee         float64
	Currency            string
	PlacedTime          time.Time
	StatusUpdateTime    time.Time
	SecurityType        string
	CounterpartyCode    string
	RawAmount           float64
	NetAmount           float64
	PNL                 float64
}

func MapHeadersToIndex(headers []string) map[string]int {
	mapping := map[string]int{
		ORDER_SUMMARIZED_HEADER:     -1,
		FINANCIAL_INSTRUMENT_HEADER: -1,
		MARKET_HEADER:               -1,
		NAME_HEADER:                 -1,
		SIDE_HEADER:                 -1,
		QUANTITY_HEADER:             -1,
		PRICE_HEADER:                -1,
		TOTAL_AMOUNT_HEADER:         -1,
		TOTAL_CHARGE_HEADER:         -1,
		GST_HEADER:                  -1,
		SEC_FEE_HEADER:              -1,
		OPTION_REGULATORY_HEADER:    -1,
		CLEARING_FEE_HEADER:         -1,
		TRADING_ACTIVITY_FEE_HEADER: -1,
		COMMISSION_HEADER:           -1,
		PLATFORM_FEE_HEADER:         -1,
		CURRENCY_HEADER:             -1,
		PLACED_TIME_HEADER:          -1,
		STATUS_UPDATE_TIME_HEADER:   -1,
		SECURITY_TYPE_HEADER:        -1,
		COUNTERPARTY_CODE_HEADER:    -1,
	}

	for index, header := range headers {
		if strings.HasPrefix(header, string('\ufeff')) {
			logger.Debug("Removing BOM from header: ", header)
			header = strings.Replace(header, string('\ufeff'), "", -1)
			header = strings.ReplaceAll(header, "\"", "")
		}

		if _, exists := mapping[header]; exists {
			mapping[header] = index
		} else {
			logger.Errorf("Header not found in constants: %v", header)
		}
	}

	return mapping
}

func DetermineTradeLeg(trades []*Trade) {
	// position is opened when there is no existing position, the position can be 'buy' or 'sell'
	// the position is closed when there is an existing position, and cumulatively, the position will offset the original opened position to 0
	positions := make(map[string]map[string]interface{})

	pnl := 0.0
	tradeNum := 0

	for _, trade := range trades {
		if position, exists := positions[trade.FinancialInstrument]; exists {
			// this is an existing position, reduce the position size or close the position
			if trade.Side == "buy" {
				position["quantity"] = position["quantity"].(int) + trade.Quantity
			} else {
				position["quantity"] = position["quantity"].(int) - trade.Quantity
			}
			position["netAmount"] = position["netAmount"].(float64) + trade.RawAmount

			// position is closed
			if position["quantity"] == 0 {
				trade.NetAmount = position["netAmount"].(float64)
				trade.PNL = pnl + position["netAmount"].(float64)
				pnl += position["netAmount"].(float64)

				logger.Printf(
					"%v, %v, %v, %v@%v trade_leg_net=%.2f, overall_pnl=%.2f",
					position["tradeNum"], trade.FinancialInstrument, trade.Side, trade.Quantity,
					trade.Price, trade.NetAmount, trade.PNL)
				delete(positions, trade.FinancialInstrument)
			} else {
				logger.Printf(
					"%v, %v, %v, %v@%v",
					position["tradeNum"], trade.FinancialInstrument, trade.Side, trade.Quantity, trade.Price)
			}
		} else {
			// this is a new position, open the position
			tradeNum++
			logger.Printf("%v, %v, %v, %v@%.2f",
				tradeNum, trade.FinancialInstrument, trade.Side, trade.Quantity, trade.Price)
			initQty := 0
			if trade.Side == "buy" {
				initQty = trade.Quantity
			} else {
				initQty = -trade.Quantity
			}
			positions[trade.FinancialInstrument] = map[string]interface{}{
				"quantity":  initQty,
				"netAmount": trade.RawAmount,
				"tradeNum":  tradeNum,
			}
		}
	}

	logger.Info("Total trades executed: ", tradeNum-1)
}

func (t *Trade) calculateRawAmount() {
	multiplier := 1
	if t.Side == "buy" {
		multiplier = -1
	}
	t.RawAmount = (t.TotalAmount * float64(multiplier)) - t.TotalCharge
}

func SortTradesByStatusUpdateTime(trades []*Trade) {
	sort.Slice(trades, func(i, j int) bool {
		return trades[i].StatusUpdateTime.Before(trades[j].StatusUpdateTime)
	})
}

func ParseFloatValueIfExists(value string) float64 {
	if value == "" {
		return 0.0
	}

	floatValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		logger.Error("Error converting string to float64:", err)
	}

	return floatValue
}

func ingestCSV(filePath string) []*Trade {
	file, err := os.Open(filePath)
	if err != nil {
		logger.Error("Error opening file:", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		logger.Error("Error reading CSV:", err)
	}

	trades := []*Trade{}

	count := 0
	logger.Debug("Total raw records: ", len(records))
	indexOf := make(map[string]int)
	for i, row := range records {
		if i == 0 {
			indexOf = MapHeadersToIndex(row)
		}

		if i != 0 && row[0] != "" && row[indexOf[SECURITY_TYPE_HEADER]] == "OPT" {
			quantity, err := strconv.Atoi(row[indexOf[QUANTITY_HEADER]])
			if err != nil {
				logger.Error("Error converting quantity to int:", err)
			}

			price, err := strconv.ParseFloat(row[indexOf[PRICE_HEADER]], 64)
			if err != nil {
				logger.Error("Error converting price to float64:", err)
			}

			totalAmount, err := strconv.ParseFloat(row[indexOf[TOTAL_AMOUNT_HEADER]], 64)
			if err != nil {
				logger.Error("Error converting total amount to float64:", err)
			}

			totalCharge, err := strconv.ParseFloat(row[indexOf[TOTAL_CHARGE_HEADER]], 64)
			if err != nil {
				logger.Error("Error converting total charge to float64:", err)
			}

			gst, err := strconv.ParseFloat(row[indexOf[GST_HEADER]], 64)
			if err != nil {
				logger.Error("Error converting gst to float64:", err)
			}

			secFee := ParseFloatValueIfExists(row[indexOf[SEC_FEE_HEADER]])

			optionRegulatoryFee, err := strconv.ParseFloat(row[indexOf[OPTION_REGULATORY_HEADER]], 64)
			if err != nil {
				logger.Error("Error converting option regulatory fee to float64:", err)
			}

			clearingFee, err := strconv.ParseFloat(row[indexOf[CLEARING_FEE_HEADER]], 64)
			if err != nil {
				logger.Error("Error converting clearing fee to float64:", err)
			}

			tradingActivityFee := ParseFloatValueIfExists(row[indexOf[TRADING_ACTIVITY_FEE_HEADER]])

			commission, err := strconv.ParseFloat(row[indexOf[COMMISSION_HEADER]], 64)
			if err != nil {
				logger.Error("Error converting commission to float64:", err)
			}

			platformFee, err := strconv.ParseFloat(row[indexOf[PLATFORM_FEE_HEADER]], 64)
			if err != nil {
				logger.Error("Error converting platform fee to float64:", err)
			}

			placedTime, err := time.Parse(TIGER_TRADE_DATETIME_FORMAT, row[indexOf[PLACED_TIME_HEADER]])
			if err != nil {
				logger.Error("Error converting placed time to time.Time:", err)
			}

			statusUpdateTime, err := time.Parse(TIGER_TRADE_DATETIME_FORMAT, row[indexOf[STATUS_UPDATE_TIME_HEADER]])
			if err != nil {
				logger.Error("Error converting status update time to time.Time:", err)
			}

			trade := &Trade{
				FinancialInstrument: row[indexOf[FINANCIAL_INSTRUMENT_HEADER]],
				Market:              row[indexOf[MARKET_HEADER]],
				Name:                row[indexOf[NAME_HEADER]],
				Side:                row[indexOf[SIDE_HEADER]],
				Quantity:            quantity,
				Price:               price,
				TotalAmount:         totalAmount,
				TotalCharge:         totalCharge,
				GST:                 gst,
				SECFee:              secFee,
				OptionRegulatoryFee: optionRegulatoryFee,
				ClearingFee:         clearingFee,
				TradingActivityFee:  tradingActivityFee,
				Commission:          commission,
				PlatformFee:         platformFee,
				Currency:            row[indexOf[CURRENCY_HEADER]],
				PlacedTime:          placedTime,
				StatusUpdateTime:    statusUpdateTime,
				SecurityType:        row[indexOf[SECURITY_TYPE_HEADER]],
				CounterpartyCode:    row[indexOf[COUNTERPARTY_CODE_HEADER]],
			}
			trade.calculateRawAmount()

			trades = append(trades, trade)
			logger.Debug(row)
			logger.Debug(trades[count])
			count++
		}
	}

	logger.Info("Total executions: ", count)

	return trades
}
