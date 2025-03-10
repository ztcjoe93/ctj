package main

import (
	"testing"
)

func TestMapHeadersToIndex(t *testing.T) {
	headers := []string{
		"Order Summarized",
		"Financial Instrument",
		"Market",
		"Name",
		"Side",
		"Quantity",
		"Price",
		"Total Amount",
		"Total Charge",
		"GST",
		"SEC Fee",
		"Option Regulatory Fee",
		"Clearing Fee",
		"Trading Activity Fee",
		"Commission",
		"Platform Fee",
		"Currency",
		"Placed Time",
		"Status Update Time",
		"Security Type",
		"Counterparty Code",
	}
	mapping := MapHeadersToIndex(headers)
	if mapping["Order Summarized"] != 0 {
		t.Errorf("Expected 0, got %d", mapping["Order Summarized"])
	}
}
